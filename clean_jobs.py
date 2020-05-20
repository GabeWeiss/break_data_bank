import re
import subprocess

proc = subprocess.run(["kubectl get jobs | grep test"], shell=True, capture_output=True, text=True)

out = proc.stdout

jobs = out.split('\n')

for job in jobs:
    x = re.match("(test-[^ ]+).*", job)
    if not x:
        continue
    sProc = subprocess.run([f"kubectl delete job {x.group(1)}"], shell=True, capture_output=True, text=True)
