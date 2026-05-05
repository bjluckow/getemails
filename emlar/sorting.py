from dataclasses import dataclass
from enum import Enum

class DateGrouping(str, Enum):
    day = "day"
    month = "month"
    year = "year"

@dataclass
class SortingSpec:
    groupby_date: DateGrouping | None = None
    groupby_folder: bool = False
    groupby_thread: bool = False