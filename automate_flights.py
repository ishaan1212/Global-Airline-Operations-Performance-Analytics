import schedule # type: ignore
import time
import os

def run_script():
    os.system("python upload_flights.py")  # Update with the actual path

# Run the script every 5 minutes
schedule.every(1).minutes.do(run_script)

while True:
    schedule.run_pending()
    time.sleep(60)  # Wait 60 seconds before checking again
