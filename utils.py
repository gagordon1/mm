import time
import pathlib
import datetime

def get_current_utc_nanoseconds():
    """Returns the current UTC time as nanoseconds since the epoch."""
    return int(time.time_ns())

def get_daily_filename(output_dir: pathlib.Path, venue: str) -> pathlib.Path:
    """Generates a filename based on the current UTC date and venue."""
    now_utc = datetime.datetime.utcnow()
    date_str = now_utc.strftime('%Y-%m-%d')
    # Include venue in filename for clarity when multiple venues are collected
    filename = f"{venue}_{date_str}.parquet" 
    return output_dir / filename