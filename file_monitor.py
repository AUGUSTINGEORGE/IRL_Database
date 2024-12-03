import os
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from database_setup import populate_data


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class ExcelFileChangeHandler(FileSystemEventHandler):
    """
    Event handler to monitor changes to a specified Excel file.
    """
    def __init__(self, file_path):
        self.file_path = os.path.abspath(file_path)
        self.last_trigger_time = 0

    def on_modified(self, event):
        """
        Called when a file is modified. Checks if the target file is updated.
        """
        logging.info(f"Event detected: {event.src_path}")
        monitored_file = self.file_path

        # Ignore temporary or backup files
        if event.src_path.endswith("~$") or not event.src_path.endswith(".xlsx"):
            logging.info("Temporary or non-Excel file detected. Skipping.")
            return

        # Check if the modified file matches the monitored file
        if os.path.abspath(event.src_path) == monitored_file:
            current_time = time.time()
            debounce_interval = 5  # Seconds to debounce multiple events

            # Debounce to avoid processing multiple events from the same save action
            if current_time - self.last_trigger_time > debounce_interval:
                self.last_trigger_time = current_time
                logging.info(f"File was saved: {event.src_path}. Proceeding to update the database.")
                self.process_file()
            else:
                logging.info("Debounced duplicate event. Skipping.")

    def process_file(self):
        """
        Calls the populate_data function to update the database.
        """
        try:
            logging.info("Calling populate_data()...")
            populate_data()
            logging.info("Database updated successfully!")
        except Exception as e:
            logging.error(f"Error while updating the database: {e}")


def monitor_excel_file(file_path):
    """
    Starts monitoring the specified Excel file for changes.
    """
    if not os.path.exists(file_path):
        logging.error(f"Error: File {file_path} does not exist.")
        return

    file_path = os.path.abspath(file_path)
    event_handler = ExcelFileChangeHandler(file_path)
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(file_path), recursive=False)
    observer.start()
    logging.info(f"Monitoring {file_path} for changes. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logging.info("Stopping file monitoring...")
    observer.join()


if __name__ == "__main__":
    # Hardcoded file path for monitoring
    EXCEL_FILE_PATH = "C:/Users/saint/Downloads/IRL_setup-main/IRL_setup-main/decodefactsheet_copy.xlsx"

    # Start monitoring the Excel file
    monitor_excel_file(EXCEL_FILE_PATH)
