import pandas as pd
from structlog import get_logger
from etl.snapshot import Snapshot
import numpy as np

# Initialize logger.
log = get_logger()


def load_data(snap: Snapshot) -> pd.ExcelFile:
    """
    Load the Excel file from the given snapshot.

    Args:
        snap (Snapshot): The snapshot object containing the path to the Excel file.

    Returns:
        pd.ExcelFile: The loaded Excel file as a pandas ExcelFile object, or None if loading failed.
    """

    # Attempt to load the Excel file from the snapshot path.
    try:
        excel_object = pd.ExcelFile(snap.path)
    except Exception as e:
        # Log an error and return None if loading failed.
        log.error(f"Failed to load Excel file: {e}")
        return None

    # Return the loaded Excel file as a pandas ExcelFile object.
    return excel_object