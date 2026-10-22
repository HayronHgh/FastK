use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

use fastk::{
    scoped_scalar_series_key, FastKStore, KlineRecord, MetricsLevel, RangeWriteSummary,
    ScalarPredicateExpr, ScalarPredicateQuery, ScalarRecord,
};
use numpy::{IntoPyArray, PyArray1, PyArrayMethods};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

pyo3::create_exception!(_native, FastKError, PyRuntimeError);

fn engine_error(error: fastk::FastKError) -> PyErr {
    let result = FastKError::new_err(error.to_string());
    Python::attach(|py| {
        let _ = result.value(py).setattr("code", error.code());
    });
    result
}

fn unavailable(message: &str) -> fastk::FastKError {
    fastk::FastKError::InvalidInput(message.into())
}

// The owned allocation is handed to NumPy as int64 words. NoUninit derives on
// the records reject padding; these assertions also pin the public field layout.
const _: () = {
    use std::mem::{align_of, offset_of, size_of};
    assert!(size_of::<KlineRecord>() == 48);
    assert!(size_of::<ScalarRecord>() == 16);
    assert!(align_of::<KlineRecord>() == align_of::<i64>());
    assert!(align_of::<ScalarRecord>() == align_of::<i64>());
    assert!(offset_of!(KlineRecord, ts) == 0);
    assert!(offset_of!(KlineRecord, open) == 8);
    assert!(offset_of!(KlineRecord, high) == 16);
    assert!(offset_of!(KlineRecord, low) == 24);
    assert!(offset_of!(KlineRecord, close) == 32);
    assert!(offset_of!(KlineRecord, volume) == 40);
    assert!(offset_of!(ScalarRecord, ts) == 0);
    assert!(offset_of!(ScalarRecord, value) == 8);
};

fn words<T: bytemuck::NoUninit>(rows: Vec<T>) -> fastk::Result<Vec<i64>> {
    bytemuck::allocation::try_cast_vec(rows).map_err(|(error, _)| {
        fastk::FastKError::InvalidData(format!("incompatible native record layout: {error}"))
    })
}

fn array(py: Python<'_>, words: Vec<i64>, dtype: &str) -> PyResult<Py<PyAny>> {
    let dtype = py.import("fastk_native._native")?.getattr(dtype)?;
    let base = words.into_pyarray(py);
    base.call_method1("setflags", (false,))?;
    let result = base.call_method1("view", (dtype,))?;
    result.call_method1("setflags", (false,))?;
    Ok(result.unbind())
}

/// Validate the layout before borrowing. Only a non-contiguous input is copied here.
fn input_array<'py>(
    py: Python<'py>,
    rows: &Bound<'py, PyAny>,
    dtype: &str,
) -> PyResult<Bound<'py, PyArray1<i64>>> {
    let np = py.import("numpy")?;
    let expected = py.import("fastk_native._native")?.getattr(dtype)?;
    if !rows.is_instance(&np.getattr("ndarray")?)?
        || rows.getattr("ndim")?.extract::<usize>()? != 1
        || !rows.getattr("dtype")?.eq(&expected)?
    {
        return Err(PyValueError::new_err(
            "expected a one-dimensional packed FastK int64 structured array",
        ));
    }
    let contiguous = np.call_method1("ascontiguousarray", (rows,))?;
    Ok(contiguous
        .call_method1("view", (np.getattr("int64")?,))?
        .cast_into::<PyArray1<i64>>()?)
}

fn summary(py: Python<'_>, value: RangeWriteSummary) -> PyResult<Py<PyAny>> {
    let result = PyDict::new(py);
    result.set_item("timeframe_ms", value.timeframe_ms)?;
    result.set_item("registered", value.registered)?;
    result.set_item("requested_record_count", value.requested_record_count)?;
    result.set_item("written_record_count", value.written_record_count)?;
    result.set_item("month_batches_written", value.month_batches_written)?;
    Ok(result.into_any().unbind())
}

#[pyclass(module = "fastk_native._native")]
struct Reader {
    store: RwLock<Option<FastKStore>>,
}

impl Reader {
    fn read<T: Send>(
        &self,
        py: Python<'_>,
        action: impl FnOnce(&FastKStore) -> fastk::Result<T> + Send,
    ) -> PyResult<T> {
        py.detach(|| {
            let guard = self
                .store
                .read()
                .map_err(|_| unavailable("reader poisoned; close and reopen"))?;
            action(
                guard
                    .as_ref()
                    .ok_or_else(|| unavailable("reader is closed"))?,
            )
        })
        .map_err(engine_error)
    }
}

#[pymethods]
impl Reader {
    #[new]
    #[pyo3(signature = (root, *, threads=1))]
    fn new(py: Python<'_>, root: PathBuf, threads: usize) -> PyResult<Self> {
        if threads == 0 {
            return Err(PyValueError::new_err("threads must be positive"));
        }
        let root = std::path::absolute(root).map_err(|error| engine_error(error.into()))?;
        let store = py
            .detach(|| {
                if !root.join(".fastk.write.lock").try_exists()? {
                    return Err(unavailable(
                        "initialize the store before opening a native reader",
                    ));
                }
                let mut store = FastKStore::open_read_only(root)?;
                store.set_read_threads(threads)?;
                store.set_metrics_level(MetricsLevel::Off);
                Ok::<_, fastk::FastKError>(store)
            })
            .map_err(engine_error)?;
        Ok(Self {
            store: RwLock::new(Some(store)),
        })
    }

    fn read_kline_range(
        &self,
        py: Python<'_>,
        symbol: &str,
        timeframe: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> PyResult<Py<PyAny>> {
        let words = self.read(py, |store| {
            words(store.get_kline_range(symbol, timeframe, start_ts, end_ts)?)
        })?;
        array(py, words, "KLINE_DTYPE")
    }

    fn read_kline_latest(
        &self,
        py: Python<'_>,
        symbol: &str,
        timeframe: &str,
        limit: usize,
    ) -> PyResult<Py<PyAny>> {
        let words = self.read(py, |store| {
            words(store.get_kline_latest_n(symbol, timeframe, limit)?)
        })?;
        array(py, words, "KLINE_DTYPE")
    }

    fn read_scalar_range(
        &self,
        py: Python<'_>,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> PyResult<Py<PyAny>> {
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        let words = self.read(py, |store| {
            words(store.get_scalar_range(&key, start_ts, end_ts)?)
        })?;
        array(py, words, "SCALAR_DTYPE")
    }

    #[pyo3(signature = (symbol, timeframe, category, name, start_ts, end_ts, *, predicate, value=None, min=None, max=None, values=None, inclusive=true))]
    fn query_scalar_predicate(
        &self,
        py: Python<'_>,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
        start_ts: i64,
        end_ts: i64,
        predicate: &str,
        value: Option<i64>,
        min: Option<i64>,
        max: Option<i64>,
        values: Option<Vec<i64>>,
        inclusive: bool,
    ) -> PyResult<Py<PyAny>> {
        let required = |value: Option<i64>| {
            value.ok_or_else(|| PyValueError::new_err("missing predicate value or bound"))
        };
        let predicate = match predicate {
            "eq" => ScalarPredicateExpr::Eq(required(value)?),
            "ne" => ScalarPredicateExpr::Ne(required(value)?),
            "gt" => ScalarPredicateExpr::Gt(required(value)?),
            "gte" => ScalarPredicateExpr::Gte(required(value)?),
            "lt" => ScalarPredicateExpr::Lt(required(value)?),
            "lte" => ScalarPredicateExpr::Lte(required(value)?),
            "between" => ScalarPredicateExpr::Between {
                min: required(min)?,
                max: required(max)?,
                inclusive,
            },
            "in" => ScalarPredicateExpr::InSet(
                values.ok_or_else(|| PyValueError::new_err("missing values"))?,
            ),
            "not-in" => ScalarPredicateExpr::NotInSet(
                values.ok_or_else(|| PyValueError::new_err("missing values"))?,
            ),
            _ => return Err(PyValueError::new_err("unknown scalar predicate")),
        };
        predicate.validate().map_err(engine_error)?;
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        let words = self.read(py, move |store| {
            let result = store.query_scalar_predicate(ScalarPredicateQuery {
                key,
                start_ts,
                end_ts,
                predicate,
                return_values: true,
            })?;
            let mut words = Vec::with_capacity(result.matches.len() * 2);
            for row in result.matches {
                words.push(row.ts);
                words.push(row.value.ok_or_else(|| {
                    fastk::FastKError::InvalidData(
                        "predicate result missing requested value".into(),
                    )
                })?);
            }
            Ok(words)
        })?;
        array(py, words, "SCALAR_DTYPE")
    }

    fn kline_info(&self, py: Python<'_>, symbol: &str, timeframe: &str) -> PyResult<Py<PyAny>> {
        let meta = self.read(py, |store| store.kline_metadata(symbol, timeframe))?;
        let result = PyDict::new(py);
        result.set_item("symbol", &meta.symbol)?;
        result.set_item("timeframe", timeframe)?;
        result.set_item("timeframe_ms", meta.timeframe_ms)?;
        result.set_item("price_scale", meta.price_scale)?;
        result.set_item("volume_scale", meta.volume_scale)?;
        result.set_item("timestamp_unit", "ms")?;
        result.set_item("schema_id", meta.schema_id)?;
        Ok(result.into_any().unbind())
    }

    fn scalar_info(
        &self,
        py: Python<'_>,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
    ) -> PyResult<Py<PyAny>> {
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        let meta = self.read(py, |store| store.scalar_metadata(&key))?;
        let result = PyDict::new(py);
        result.set_item("symbol", symbol)?;
        result.set_item("timeframe", timeframe)?;
        result.set_item("category", category)?;
        result.set_item("name", name)?;
        result.set_item("timeframe_ms", meta.timeframe_ms)?;
        result.set_item("timestamp_unit", "ms")?;
        result.set_item("schema_id", meta.schema_id)?;
        Ok(result.into_any().unbind())
    }

    fn close(&self, py: Python<'_>) {
        py.detach(|| {
            self.store.write().unwrap_or_else(|e| e.into_inner()).take();
        });
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf
            .store
            .read()
            .map_err(|_| PyRuntimeError::new_err("reader poisoned"))?
            .is_none()
        {
            return Err(PyRuntimeError::new_err("reader is closed"));
        }
        Ok(slf)
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) {
        self.close(py);
    }
}

#[pyclass(module = "fastk_native._native")]
struct Writer {
    store: Mutex<Option<FastKStore>>,
}

impl Writer {
    fn write<T: Send>(
        &self,
        py: Python<'_>,
        action: impl FnOnce(&mut FastKStore) -> fastk::Result<T> + Send,
    ) -> PyResult<T> {
        py.detach(|| {
            let mut guard = self
                .store
                .lock()
                .map_err(|_| unavailable("writer poisoned; close and reopen"))?;
            action(
                guard
                    .as_mut()
                    .ok_or_else(|| unavailable("writer is closed"))?,
            )
        })
        .map_err(engine_error)
    }
}

#[pymethods]
impl Writer {
    #[new]
    fn new(py: Python<'_>, root: PathBuf) -> PyResult<Self> {
        let root = std::path::absolute(root).map_err(|error| engine_error(error.into()))?;
        let store = py
            .detach(|| {
                let mut store = FastKStore::open(&root)?;
                if !root.join("series").exists() || !root.join(".fastk.write.lock").exists() {
                    store.init()?;
                }
                Ok::<_, fastk::FastKError>(store)
            })
            .map_err(engine_error)?;
        Ok(Self {
            store: Mutex::new(Some(store)),
        })
    }

    #[pyo3(signature = (symbol, timeframe, rows, *, timeframe_ms, price_scale, volume_scale))]
    fn write_kline_range(
        &self,
        py: Python<'_>,
        symbol: &str,
        timeframe: &str,
        rows: &Bound<'_, PyAny>,
        timeframe_ms: i64,
        price_scale: i64,
        volume_scale: i64,
    ) -> PyResult<Py<PyAny>> {
        let input = input_array(py, rows, "KLINE_DTYPE")?;
        let records: Vec<_> = input
            .readonly()
            .as_slice()?
            .chunks_exact(6)
            .map(|r| KlineRecord {
                ts: r[0],
                open: r[1],
                high: r[2],
                low: r[3],
                close: r[4],
                volume: r[5],
            })
            .collect();
        let result = self.write(py, |store| {
            store.append_kline_range(
                symbol,
                timeframe,
                &records,
                timeframe_ms,
                price_scale,
                volume_scale,
            )
        })?;
        summary(py, result)
    }

    #[pyo3(signature = (symbol, timeframe, category, name, rows, *, timeframe_ms))]
    fn write_scalar_range(
        &self,
        py: Python<'_>,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
        rows: &Bound<'_, PyAny>,
        timeframe_ms: i64,
    ) -> PyResult<Py<PyAny>> {
        let input = input_array(py, rows, "SCALAR_DTYPE")?;
        let records: Vec<_> = input
            .readonly()
            .as_slice()?
            .chunks_exact(2)
            .map(|r| ScalarRecord {
                ts: r[0],
                value: r[1],
            })
            .collect();
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        let result = self.write(py, |store| {
            store.append_scalar_range(&key, &records, Some(timeframe_ms))
        })?;
        summary(py, result)
    }

    fn close(&self, py: Python<'_>) {
        py.detach(|| {
            self.store.lock().unwrap_or_else(|e| e.into_inner()).take();
        });
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf
            .store
            .lock()
            .map_err(|_| PyRuntimeError::new_err("writer poisoned"))?
            .is_none()
        {
            return Err(PyRuntimeError::new_err("writer is closed"));
        }
        Ok(slf)
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) {
        self.close(py);
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    let np = py.import("numpy")?;
    module.add(
        "KLINE_DTYPE",
        np.call_method1(
            "dtype",
            (vec![
                ("ts", "<i8"),
                ("open", "<i8"),
                ("high", "<i8"),
                ("low", "<i8"),
                ("close", "<i8"),
                ("volume", "<i8"),
            ],),
        )?,
    )?;
    module.add(
        "SCALAR_DTYPE",
        np.call_method1("dtype", (vec![("ts", "<i8"), ("value", "<i8")],))?,
    )?;
    module.add("FastKError", py.get_type::<FastKError>())?;
    module.add_class::<Reader>()?;
    module.add_class::<Writer>()?;
    module.add(
        "__version__",
        env!("CARGO_PKG_VERSION").replace("-rc.", "rc"),
    )?;
    Ok(())
}

#[cfg(test)]
mod allocation_tests {
    use super::*;

    #[test]
    fn kline_handoff_preserves_allocation_capacity_and_field_order() {
        let mut rows = Vec::with_capacity(7);
        rows.push(KlineRecord {
            ts: i64::MIN,
            open: i64::MAX,
            high: 1 << 54,
            low: -7,
            close: 11,
            volume: 0,
        });
        let pointer = rows.as_ptr().cast::<i64>();
        let capacity = rows.capacity() * 6;
        let result = words(rows).unwrap();
        assert_eq!(result.as_ptr(), pointer);
        assert_eq!(result.capacity(), capacity);
        assert_eq!(result, [i64::MIN, i64::MAX, 1 << 54, -7, 11, 0]);
    }

    #[test]
    fn scalar_and_empty_handoff_keep_the_original_allocation() {
        let mut rows = Vec::with_capacity(11);
        rows.push(ScalarRecord {
            ts: i64::MAX,
            value: i64::MIN,
        });
        let pointer = rows.as_ptr().cast::<i64>();
        let capacity = rows.capacity() * 2;
        let result = words(rows).unwrap();
        assert_eq!(result.as_ptr(), pointer);
        assert_eq!(result.capacity(), capacity);
        assert_eq!(result, [i64::MAX, i64::MIN]);

        for capacity in [0, 7] {
            let rows = Vec::<KlineRecord>::with_capacity(capacity);
            let pointer = rows.as_ptr().cast::<i64>();
            let result = words(rows).unwrap();
            assert_eq!(result.as_ptr(), pointer);
            assert_eq!(result.capacity(), capacity * 6);
            assert!(result.is_empty());
        }
        assert!(words(Vec::<ScalarRecord>::new()).unwrap().is_empty());
    }

    #[test]
    fn incompatible_layout_returns_an_error() {
        let error = words(vec![0_u8; 8]).unwrap_err();
        assert_eq!(error.code(), "INVALID_DATA");
    }
}
