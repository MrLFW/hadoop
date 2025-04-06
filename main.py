#!/usr/bin/env python3
import subprocess
import sys


def run_job(cmd, allow_fail=False):
    print("Running:", cmd)
    ret = subprocess.call(cmd, shell=True)
    if ret != 0 and not allow_fail:
        sys.exit(ret)


def run_job_and_capture(cmd):
    print("Running:", cmd)
    out = subprocess.check_output(cmd, shell=True)
    return out.decode()


def main():
    logs_hdfs = "/user/luke/logs/logs.txt"
    job1_out = "/user/luke/job1_output"
    job2_out = "/user/luke/job2_output"
    job3_out = "/user/luke/job3_output"

    for path in [job1_out, job2_out, job3_out]:
        run_job(f"hdfs dfs -rm -r {path}", allow_fail=True)

    job1_cmd = (
        "hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar "
        f"-input {logs_hdfs} -output {job1_out} "
        "-mapper 'python3 job1_mapper.py' -reducer 'python3 job1_reducer.py' "
        "-file job1_mapper.py -file job1_reducer.py"
    )
    run_job(job1_cmd)

    run_job(f"hdfs dfs -get {job1_out} ./job1_output")
    peak_hour = None
    max_count = -1
    with open("job1_output/part-00000", "r") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 2:
                continue
            hour, count_str = parts
            try:
                count = int(count_str)
            except Exception:
                continue
            if count > max_count:
                max_count = count
                peak_hour = hour
    if peak_hour is None:
        print("failed to find peak hour")
        return

    print("peak hour:", peak_hour)

    job2_cmd = (
        "hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar "
        f"-input {logs_hdfs} -output {job2_out} "
        f"-mapper 'python3 job2_mapper.py {peak_hour}' -reducer 'python3 job2_reducer.py' "
        "-file job2_mapper.py -file job2_reducer.py"
    )
    run_job(job2_cmd)

    job3_cmd = (
        "hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar "
        f"-input {job2_out} -output {job3_out} "
        "-mapper 'python3 job3_mapper.py' -reducer 'python3 job3_reducer.py' "
        "-file job3_mapper.py -file job3_reducer.py"
    )
    run_job(job3_cmd)

    final_output = run_job_and_capture(f"hdfs dfs -cat {job3_out}/part-*")
    print(f"Final output:\n{final_output}")
    run_job(f"hdfs dfs -get {job3_out} ./job3_output")


if __name__ == "__main__":
    main()
