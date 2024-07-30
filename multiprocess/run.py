import os
import subprocess
import time
import glob
from typing import Optional, Tuple, Union, List
import numpy as np
import pandas as pd


def run_one_cmd(cmd=['python', './test.py'], index=None, log_dir=None, time_sleep=30):
    # while True:
    proc = subprocess.Popen(args=cmd, encoding='utf-8')
    return proc
        # print(proc.pid)
        # while proc.poll() == None:
        #     time.sleep(time_sleep)
        #     # print('running')
        # if proc.poll() == 0:
        #     print(f'{cmd_one_line} finish')
        #     if index > 0 and isinstance(index, int):
        #         log_file = 'work_' + "{:0>5d}".format(index) + '.log'
        #         f = open(f'{log_dir}/{log_file}', 'wt')
        #         f.write(cmd_one_line + '\n')
        #         f.close()
        #     break
        # else:
        #     print(f"{cmd_one_line} stop")
        #     continue

def read_cmd_file(
    cmd_file: str
) -> pd.DataFrame:
    f = open(cmd_file, 'rt')
    cmds = f.readlines()
    cmds = [i.strip() for i in cmds]
    cmds = [" ".join(one_line.split()) for one_line in cmds] # remove more than one consecutive space as one
    cmd_df = pd.DataFrame({'cmd':cmds, 'index': range(1, len(cmds)+1)})
    return cmd_df

def prepare_log_dir(
    cmd_file: str
) -> str:
    dir_name = os.path.basename(cmd_file) + '.log'
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    return dir_name

def collect_finished_cmds(
    log_dir: str
) -> List:
    finished_log_files = glob.glob(f'{log_dir}/work*.log')
    finished_cmds = np.array([open(i, 'rt').readlines() for i in finished_log_files]).flatten()
    finished_cmds = [i.strip() for i in finished_cmds]
    return finished_cmds

def get_running_num(
    task_dict: dict,
):
    update_task_dict(task_dict=task_dict)
    if len(task_dict) == 0:
        return 0
    running_num = 0
    for proc, proc_obj in task_dict.items():
        if proc_obj['state'] == None:
            running_num += 1
    return running_num


def update_task_dict(task_dict):
    if len(task_dict) == 0:
        return None
    for proc, proc_obj in task_dict.items():
        proc_obj['state'] = proc.poll()

def save_finish_job_to_log(task_dict, log_dir):
    for proc, proc_obj in task_dict.items():
        log_file = 'work_' + "{:0>5d}".format(proc_obj['index']) + '.log'
        if proc_obj['state'] == 0 and not os.path.exists(f'{log_dir}/{log_file}'):
            f = open(f'{log_dir}/{log_file}', 'wt')
            f.write(proc_obj['cmd_line'] + '\n')
            f.close()


def add_one_job_to_task_dict(task_dict, proc, cmd_line, index):
    task_dict[proc] = {}
    task_dict[proc]['state'] = proc.poll()
    task_dict[proc]['cmd_line'] = cmd_line
    task_dict[proc]['index'] = index

def _run_cmds(
    cmd_file: str,
    task_num: int = 1,
    time_sleep: int = 30,
):
    cmd_df = read_cmd_file(cmd_file)
    log_dir = prepare_log_dir(cmd_file)
    finished_cmds = collect_finished_cmds(log_dir)
    cmds = np.setdiff1d(cmd_df['cmd'],finished_cmds)
    cmd_df = cmd_df[np.isin(cmd_df['cmd'], cmds)]

    task_dict = {}
    for indx, one_row in cmd_df.iterrows():
        cmd_line, index = one_row['cmd'], one_row['index']
        while True:
            if get_running_num(task_dict=task_dict) < task_num:
                proc = run_one_cmd(cmd=cmd_line.split())
                add_one_job_to_task_dict(task_dict=task_dict, proc=proc, cmd_line=cmd_line, index=index)
                break
            else:
                time.sleep(time_sleep)
        save_finish_job_to_log(task_dict=task_dict, log_dir=log_dir)
    while True:
        if get_running_num(task_dict=task_dict) == 0:
            break
        else:
            time.sleep(time_sleep)
    
    save_finish_job_to_log(task_dict=task_dict, log_dir=log_dir)


def run_cmds(
    cmd_file: str,
    task_num: int = 1,
    time_sleep: int = 30,
    try_time: int = 10,
):
    for i in range(try_time):
        _run_cmds(cmd_file=cmd_file, task_num=task_num, time_sleep=time_sleep)
        

        
        
