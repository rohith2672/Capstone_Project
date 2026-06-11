import pandas as pd
from typing import Tuple
from etl.models import AnomalyRecord

class DataValidator:
    def __init__(self, run_context: dict):
        """run_context contains etl_run_id, etl_run_date, etl_run_ts"""
        self.context = run_context

    def validate_weblogs(self, chunk: pd.DataFrame, source_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        anomalies = []
        clean_rows = []
        
        for idx, row in chunk.iterrows():
            reason = None
            if pd.isna(row.get('session_id')):
                reason = "null session_id"
            elif pd.isna(row.get('user_id')):
                reason = "null user_id"
            else:
                try:
                    pd.to_datetime(row.get('timestamp'))
                except:
                    reason = "invalid timestamp"
                    
            if reason:
                anomaly = AnomalyRecord.from_row(**self.context, source=source_file, idx=idx, reason=reason, row=row.to_dict())
                anomalies.append(anomaly.to_dict())
            else:
                clean_rows.append(row)
                
        return pd.DataFrame(clean_rows), pd.DataFrame(anomalies)

    def validate_users(self, df: pd.DataFrame, source_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        anomalies = []
        clean_rows = []
        for idx, row in df.iterrows():
            if pd.isna(row.get('user_name')):
                anomaly = AnomalyRecord.from_row(**self.context, source=source_file, idx=idx, reason="null user_name", row=row.to_dict())
                anomalies.append(anomaly.to_dict())
            else:
                clean_rows.append(row)
        return pd.DataFrame(clean_rows), pd.DataFrame(anomalies)

    def validate_products(self, df: pd.DataFrame, source_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        anomalies = []
        clean_rows = []
        for idx, row in df.iterrows():
            if pd.isna(row.get('price')):
                anomaly = AnomalyRecord.from_row(**self.context, source=source_file, idx=idx, reason="null price", row=row.to_dict())
                anomalies.append(anomaly.to_dict())
            else:
                clean_rows.append(row)
        return pd.DataFrame(clean_rows), pd.DataFrame(anomalies)
