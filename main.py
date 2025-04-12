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


def job_cmd(input_path, output_path, mapper, reducer, args=""):
    return (
        f"hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar "
        f"-input {input_path} -output {output_path} "
        f"-mapper 'python3 {mapper}{args}' -reducer 'python3 {reducer}' "
        f"-file {mapper} -file {reducer}"
    )


def main():
    logs_hdfs = "/user/luke/logs/logs.txt"
    job1_out = "/user/luke/job1_output"
    job2_out = "/user/luke/job2_output"
    job3_out = "/user/luke/job3_output"

    for path in [job1_out, job2_out, job3_out]:
        run_job(f"hdfs dfs -rm -r {path}", allow_fail=True)

    job1_cmd = job_cmd(logs_hdfs, job1_out, "job1_mapper.py", "job1_reducer.py")
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

    job2_cmd = job_cmd(
        logs_hdfs,
        job2_out,
        "job2_mapper.py",
        "job2_reducer.py",
        f" {peak_hour}",
    )
    run_job(job2_cmd)
    run_job(f"hdfs dfs -get {job2_out} ./job2_output")

    job3_cmd = job_cmd(
        job2_out,
        job3_out,
        "job3_mapper.py",
        "job3_reducer.py",
    )
    run_job(job3_cmd)
    run_job(f"hdfs dfs -get {job3_out} ./job3_output")

    final_output = run_job_and_capture(f"hdfs dfs -cat {job3_out}/part-*")
    print(f"Final output:\n{final_output}")


if __name__ == "__main__":
    main()
