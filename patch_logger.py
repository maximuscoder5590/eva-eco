from pathlib import Path
p = Path('/app/app.py')
s = p.read_text(encoding='utf-8')
patched = s.replace("logging.getLogger('orchestrator')", "logging.getLogger()")
if s == patched:
    print('NO-CHANGE')
else:
    p.write_text(patched, encoding='utf-8')
    print('PATCH-APPLIED')
