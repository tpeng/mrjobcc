import sys

from cc import CCJob

if __name__ == "__main__":
    components = {}

    job = CCJob(args=[sys.argv[1]])

    with job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            k, v = job.parse_output_line(line)
            components.setdefault(v, [v]).append(k)

    print components