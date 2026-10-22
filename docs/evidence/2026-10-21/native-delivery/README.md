# v0.2 development delivery verification

The final Windows GNU CLI/core archive, Linux static-musl CLI/core archive and
four-ABI native wheel bundle pass manifest/schema/SHA checks. All ZIP members
match the unpacked delivery and every nested documentation evidence manifest.
verification.json identifies their final archive hashes and test boundaries.
No package was published. Windows MSVC CI is authored but not executed locally.

| Check | Command | Exit | Evidence | Result |
|---|---|---|---|---|
| Windows archive | unittest test_contracts, final packaged CLI + manifest | 0 | final-package-hashes-windows log | PASS |
| Linux archive | Same, static-musl CLI in container | 0 | final-package-hashes-linux log | PASS |
| Native bundle | test_contracts with native bundle manifest on both OSes | 0 | final-delivery-checks logs | PASS |
| Offline CLI installs | scripts/smoke_install.py, new outside-checkout venv | 0 | Windows elevated check log, Linux package log | PASS |
| Offline native installs | scripts/smoke_native_install.py on CP311/312 Windows/Linux | 0 | ../native-engine/validation final lease logs | PASS |
| Source style | cargo fmt; Ruff check; Ruff format --check | 0 | final-format log; 45 Python files | PASS |
| Archive and historical SHA | All ZIP members, nested manifests, prior 24 manifests/668 entries, before 110 files | 0 | verification.json | PASS |

Initial environment failures are retained: Linux's test venv did not initially
contain fastk-client, and Windows sandbox temporary-directory ACLs blocked the
child CLI. Installing the delivered client / using the normal host resolved
them without runtime changes. The initial combined Linux script therefore has
exit 1 even though its build and offline installation succeeded; final contract
and hash logs are the acceptance results.

Final delivery-only cleanup restored five historical source files from a directory
named target that the temporary Linux staging filter had omitted. Generated
Python bytecode was removed from both new crate archives. ZIPs/manifests were
regenerated and verified again. Earlier artifacts and runtime binaries were
not changed. Compiler Clippy diagnostics include 17 previously recorded warnings;
the passing source-style checks do not claim zero Clippy warnings.
