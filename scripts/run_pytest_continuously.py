import datetime
import os
import signal
import subprocess
import sys
import time


def run_pytest_continuously():
    # Create a directory for storing failure logs if it doesn't exist

    root_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(root_dir)
    log_dir = os.path.join(parent_dir, "logs")
    pytest_failure_logs_dir = os.path.join(log_dir, "pytest_failure_logs")
    os.makedirs(pytest_failure_logs_dir, exist_ok=True)

    iteration = 1
    running = True

    def signal_handler(signum, frame):
        nonlocal running
        print("\nStopping test execution gracefully...")
        running = False

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while running:
        print(f"\nIteration {iteration}")
        timestamp = datetime.datetime.now()

        # Run pytest and capture output
        result = subprocess.run(
            ["pytest", "tests/", "-v"], capture_output=True, text=True
        )

        # If the test failed
        if result.returncode != 0:
            # Create a failure log filename with timestamp
            failure_time = timestamp.strftime("%Y%m%d_%H%M%S")
            log_filename = f"{pytest_failure_logs_dir}/failure_{failure_time}_iteration_{iteration}.log"

            # Write the failure details to the log file
            with open(log_filename, "w") as f:
                f.write(f"Failure occurred at: {timestamp}\n")
                f.write(f"Iteration: {iteration}\n")
                f.write("\nSTDOUT:\n")
                f.write(result.stdout)
                f.write("\nSTDERR:\n")
                f.write(result.stderr)

            print(f"Test failed! Details written to {log_filename}")

            # You might want to add additional failure handling here
            # For example, you could break the loop or add a delay

        # Print a simple status update
        print(
            f"Iteration {iteration} completed with {'SUCCESS' if result.returncode == 0 else 'FAILURE'}"
        )

        iteration += 1

        # Optional: Add a small delay between iterations to prevent overwhelming the system
        time.sleep(0.1)


if __name__ == "__main__":
    print("Starting continuous pytest execution. Press Ctrl+C to stop.")
    run_pytest_continuously()
