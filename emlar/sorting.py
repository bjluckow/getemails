from dataclasses import dataclass


@dataclass
class SortingSpec:
    groupby_date: bool = False
    groupby_folder: bool = False
    groupby_thread: bool = False