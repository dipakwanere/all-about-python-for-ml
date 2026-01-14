import pytest 
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from datapipeline import DataProcessor
import pandas as pd
from datapipeline import DataProcessor

def test_pipeline():
    df = pd.DataFrame({
        "user_id": [1, 1, 2],
        "purchase_amount": [100, 200, 300],
        "session_duration_sec": [10, 20, 30],
        "pages_visited": [1, 2, 3]
    })
    processor = DataProcessor(df)
    final_df = (
        processor.clean()
        .add_features()
        .aggreagate_user_level()
        .get_data()
    )
    assert "total_spend" in final_df.columns
    assert "avg_engagement" in final_df.columns