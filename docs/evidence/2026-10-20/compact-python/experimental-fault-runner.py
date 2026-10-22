import json,unittest
from types import SimpleNamespace
import orjson
from fastk_client import _session
_session.json=SimpleNamespace(loads=orjson.loads,dumps=json.dumps)
result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover('external-tests',pattern='test_*.py'))
raise SystemExit(not result.wasSuccessful())
