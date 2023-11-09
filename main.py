import subprocess


def main():
    # Use Popen to run scripts in the background
    subprocess.Popen(
        ["python", "/home/discreet/Playground/pg-pipeline/app/producer.py"]
    )
    subprocess.Popen(
        ["python", "/home/discreet/Playground/pg-pipeline/app/consumer.py"]
    )


if __name__ == "__main__":
    main()
