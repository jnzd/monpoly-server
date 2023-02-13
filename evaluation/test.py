import subprocess
import requests
from time import time, sleep
import os
import pandas as pd
from tqdm import tqdm
import json


GEN = "gen/gen"
SIG = "test.sig"
WRAPPER = "src/app.py"
MONITOR_PY = "../src/monitor.py"
WRAPPER_CWD = ".."
HOSTNAME = "http://localhost:5000"
LOG_EVENTS_URL = HOSTNAME + "/log-events"
SET_POLICY_URL = HOSTNAME + "/set-policy"
CHANGE_POLICY_URL = HOSTNAME + "/change-policy"
SET_SIGNATURE_URL = HOSTNAME + "/set-signature"
RESET_EVERYTHING_URL = HOSTNAME + "/reset-everything"
START_MONITOR_URL = HOSTNAME + "/start-monitor"
STOP_MONITOR_URL = HOSTNAME + "/stop-monitor"
MONPOLY = "../monpoly_dev/monpoly"


# Generation

def generate_trace(length, trace_fn, nmax=10, n=1, binsize=1):
    r = subprocess.run([GEN, SIG, "mfotl", "trace_json", str(nmax), str(n), str(binsize), str(length), trace_fn])
    r.check_returncode()

def generate_formula(depth, formula_fn, mbound=10):
    if os.path.exists(formula_fn):
        os.remove(formula_fn)
    with open(formula_fn, 'a') as f:
        r = subprocess.run([GEN, SIG, "mfotl", "policy_mon", str(depth), str(mbound)], stdout=f)
    r.check_returncode()

# Add comment

def comment_in_or_out(file_fn, line_in, line_out):
    with open(file_fn, 'r') as f:
        lines = f.readlines()
    if line_in is not None:
        if lines[line_in][0] == '#':
            lines[line_in] = lines[line_in][1:]
        print(lines[line_in])
    if line_out is not None:
        if lines[line_in][0] != '#':
            lines[line_out] = '#' + lines[line_out]
        print(lines[line_out])
    with open(file_fn, 'w') as f:
        for line in lines:
            f.write(line)

# Basic management of the wrapper

def start_wrapper():
    p = subprocess.Popen(["python3.9", WRAPPER], cwd=WRAPPER_CWD)
    print(p.pid)
    return p

def stop_wrapper(wrapper):
    wrapper.kill()

def set_policy(formula_fn):
    r = requests.post(SET_POLICY_URL, files={"policy": open(formula_fn, 'rb')})
    assert(r.ok)

def change_policy(formula_fn):
    while True:
        r = requests.post(CHANGE_POLICY_URL, files={"policy": open(formula_fn, 'rb')})
        assert(r.ok)
        if "error" in json.loads(r.text):
            print(r.text)
            sleep(1)
        else:
            break
    return r.elapsed.total_seconds()

def set_signature():
    r = requests.post(SET_SIGNATURE_URL, files={"signature": open(SIG, 'rb')})
    assert(r.ok)

def reset_everything():
    r = requests.get(RESET_EVERYTHING_URL)
    assert(r.ok)

def start_monitor():
    r = requests.get(START_MONITOR_URL)
    assert(r.ok)

def stop_monitor():
    print('stop_monitor')
    r = requests.get(STOP_MONITOR_URL)
    print('stop_monitor ok')
    assert(r.ok)

# Test functions

# a1. Monpoly batch

def test_baseline_monpoly(formula_fn, trace_fn):
    t = time()
    r = subprocess.run([MONPOLY, "-sig", SIG, "-formula", formula_fn, "-log", trace_fn + ".monpoly.trc"])
    r.check_returncode()
    return time() - t

# a2. Monpoly line by line

def test_baseline_monpoly2(formula_fn, trace_fn):
    t = 0
    r = subprocess.Popen([MONPOLY, "-sig", SIG, "-formula", formula_fn, "-verbose"], text=True,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for _ in range(3):
        r.stdout.readline()
    with open(trace_fn + ".monpoly.trc") as f:
        lines = f.readlines()
    for line in lines:
        r.stdin.write(line + ";")
        t -= time()
        r.stdin.flush()
        out = r.stdout.readline()
        r.stdout.readline()
        t += time()
    r.kill()
    return t

# b. Wrapper

def test_wrapper(trace_fn):
    r = requests.post(LOG_EVENTS_URL, files={"events": open(trace_fn + ".monpoly.json", 'rb')})
    assert(r.ok)
    return r.elapsed.total_seconds()

# Evaluation loop

if __name__ == '__main__':
    N = 1
    LENGTHS = [4**i for i in range(9)]
    DEPTH = 5
    TEMP_FORMULA = "temp.formula"
    TEMP_NEW_FORMULA = "temp_new.formula"
    TEMP_TRACE = "temp"
    OUT_FILE_1 = "out1.csv"
    OUT_FILE_2 = "out2.csv"

    series = []

    #### RQ2

    for _ in range(N):

        generate_formula(DEPTH, TEMP_FORMULA)
        generate_formula(DEPTH, TEMP_NEW_FORMULA)

        for length in tqdm(LENGTHS):
           generate_trace(length, TEMP_TRACE)

           comment_in_or_out(MONITOR_PY, 360, 361)

           wrapper = start_wrapper()
           sleep(2)

           reset_everything()
           set_signature()
           set_policy(TEMP_FORMULA)
           start_monitor()

           test_wrapper(TEMP_TRACE)

           t_baseline = change_policy(TEMP_FORMULA)

           stop_wrapper(wrapper)
        
           comment_in_or_out(MONITOR_PY, 361, 360)

           wrapper = start_wrapper()
           sleep(2)

           reset_everything()
           set_signature()
           set_policy(TEMP_FORMULA)
           start_monitor()
        
           test_wrapper(TEMP_TRACE)

           t_opt = change_policy(TEMP_FORMULA)

           stop_wrapper(wrapper)

           datapoint = pd.Series({"length": length,
                                  "t_opt": t_opt,
                                  "t_baseline": t_baseline})

           series.append(datapoint)

    df = pd.DataFrame(series)
    df["length"] = df["length"].astype(int)
    df["t_opt_a"] = df["t_opt"] / df["length"]
    df["t_baseline_a"] = df["t_baseline"] / df["length"]
    print(df)
    df.to_csv(OUT_FILE_2)
    series = []
    
    #### RQ1

    for _ in range(N):

        #generate_formula(DEPTH, TEMP_FORMULA)
    
        for length in tqdm(LENGTHS):
           generate_trace(length, TEMP_TRACE)

           wrapper = start_wrapper()
           sleep(2)
               
           reset_everything()
           set_signature()
           set_policy(TEMP_FORMULA)
           start_monitor()

           t_wrapper = test_wrapper(TEMP_TRACE)
           stop_wrapper(wrapper)

           t_baseline  = test_baseline_monpoly(TEMP_FORMULA, TEMP_TRACE)
           t_baseline2 = test_baseline_monpoly2(TEMP_FORMULA, TEMP_TRACE)

           datapoint = pd.Series({"length": length,
                                  "t_wrapper": t_wrapper,
                                  "t_baseline": t_baseline,
                                  "t_baseline2": t_baseline2})

           series.append(datapoint)
    
    df = pd.DataFrame(series)
    df["length"] = df["length"].astype(int)
    df["t_wrapper_a"] = df["t_wrapper"] / df["length"]
    df["t_baseline_a"] = df["t_baseline"] / df["length"]
    df["t_baseline2_a"] = df["t_baseline2"] / df["length"]
    print(df)
    df.to_csv(OUT_FILE_1)
                                   
       
            

    
