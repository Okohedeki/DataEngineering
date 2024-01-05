import logging
from logging import Handler

class DatabaseLoggingHandler(Handler):
    def __init__(self, postgres_connector):
        super().__init__()
        self.postgres_connector = postgres_connector

    def emit(self, record):
        try:
            # Prepare log record data
            log_entry = {
                'timestamp': self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
                'level': record.levelname,
                'message': record.getMessage(),
                'logger_name': record.name,
                'module': record.module,
                'exception': str(record.exc_info) if record.exc_info else None
            }

            # Insert log record into the database
            self.postgres_connector.insert_batch_data('logging_requests', [log_entry], page_size=1)
        except Exception as e:
            print(f"Error in log handler: {e}")