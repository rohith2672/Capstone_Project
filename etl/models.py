from dataclasses import dataclass, asdict
from datetime import datetime
import json

@dataclass
class AnomalyRecord:
    etl_run_id: str
    etl_run_date: str
    etl_run_timestamp: str
    source_file: str
    row_index: int
    rejection_reason: str
    raw_row: str

    @classmethod
    def from_row(cls, etl_run_id: str, etl_run_date: str, etl_run_timestamp: str, source: str, idx: int, reason: str, row: dict):
        return cls(
            etl_run_id=etl_run_id,
            etl_run_date=etl_run_date,
            etl_run_timestamp=etl_run_timestamp,
            source_file=source,
            row_index=idx,
            rejection_reason=reason,
            raw_row=json.dumps(row, default=str)
        )
    
    def to_dict(self):
        return asdict(self)
