import hashlib
import json
import shutil
from pathlib import Path

root = Path(__file__).resolve().parents[1]
out = root / 'docs/evidence/2026-10-21/windows-handles'
assert not out.exists()
def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()
text = (root/'target/windows-handles-lifetime.log').read_text()
rows = [json.loads(s) for s in text.splitlines() if s.startswith('{')]
assert '1 passed; 0 failed' in text
assert len(rows)==3 and sum(r['queries'] for r in rows)==1728000
assert all(len(r['process_handles_after_waves'])==120
           and set(r['process_handles_after_waves'])=={r['process_handles_before']+65}
           and r['process_handles_after_last_owner']==r['process_handles_before'] for r in rows)
old = root/'docs/evidence/2026-10-21/native-handoff'
hashes = json.loads((old/'after-source.sha256.json').read_text())
assert all(sha(root/name)==digest for name,digest in hashes.items())
out.mkdir(parents=True)
files = ['external-tests/scaling-profile/Cargo.toml',
         'external-tests/scaling-profile/Cargo.lock',
         'external-tests/scaling-profile/README.md',
         'external-tests/scaling-profile/tests/shared_lifetime.rs',
         'docs/SHARED_READER_LIFETIME.md']
for name in files:
    dest=out/'source'/name
    dest.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(root/name,dest)
for name in ['windows-handles-lifetime.log','windows-handles-fixture-before.rs',
             'windows-handles-manifest-before.toml','windows-handles-report-before.md']:
    shutil.copy2(root/'target'/name,out/name)
(out/'summary.json').write_text(json.dumps(dict(
    platform='Windows GNU x86_64, Rust 1.97.1',
    waves_per_worker_configuration=120,workers=[1,4,8],queries=1728000,
    rows=rows,scope='whole-process kernel handles, quiescent after worker joins; not in-flight peak',
    previous_frozen_sources_unchanged=len(hashes),passed=True),indent=2)+'\n')
(out/'source.sha256.json').write_text(json.dumps({n:sha(root/n) for n in files},indent=2)+'\n')
(out/'README.md').write_text('''# Windows GNU handle lifecycle evidence

The public Arc<FastKStore> fixture uses GetProcessHandleCount on the current process.
Each of 1/4/8 workers configurations executes576000queries over120waves.
All360quiescent measurements are134handles, baseline69; after each last owner
drops, count returns69. Total1728000full-record comparisons,90.70s test duration.
Read lease blocks append while retained; explicit append after last owner succeeds.
Owned records survive store drop. Root FD remains null onWindows and is not used
as a Windows counter. This is whole-process handles, not a per-path file list or
an in-flight peak; finite duration does not establish indefinite leak freedom.

Rerun with FASTK_LIFETIME_WAVES=120:
cargo test --release --locked --offline --manifest-path external-tests/scaling-profile/Cargo.toml --test shared_lifetime -- --nocapture

Only the independent test manifest/fixture/documentation changed. Windows API
dependency is a target-specific dev dependency. Product/native implementation
remains identical to all77frozen source hashes in the preceding native-handoff
evidence. No additional package/lockfile change was needed. Prior evidence stays
unchanged; before fixture/manifest/report and new exact sources are included.
This run is GNU, not MSVC. User deferred nativeLinux/MSVC until available.
SHA256SUMS.json covers every other file.
''',encoding='utf-8')
shutil.copy2(__file__,out/'export_windows_handles.py')
hashes={str(p.relative_to(out)).replace('\\','/'):sha(p) for p in sorted(out.rglob('*')) if p.is_file()}
(out/'SHA256SUMS.json').write_text(json.dumps(hashes,indent=2)+'\n')
print(json.dumps(dict(files=len(hashes),queries=1728000,passed=True)))
