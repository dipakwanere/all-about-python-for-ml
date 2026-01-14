import pandas as pd

# Data Processing class 
# input - which takes the df create a copy of it.
# process - perform the data clearning and feature engineering ops
# output - return the clean data which then can be used for model training and dev

class DataProcessor:
    @staticmethod
    def read_csv_file(filepath, **kwargs):
        return pd.read_csv(filepath, **kwargs)

    def __init__(self, df):
        self.df = df.copy()
    
    def clean(self):
        self.df = self.df.dropna(subset=["user_id"])
        self.df["purchase_amount"] = self.df["purchase_amount"].clip(lower=0)
        return self
    
    def add_features(self):
        self.df["engagement_score"] = (
            self.df["session_duration_sec"] * self.df["pages_visited"]
        )
        self.df["high_value_flag"] = (self.df["purchase_amount"] > 10000)
        return self 
    def aggreagate_user_level(self):
        agg = (
            self.df.groupby("user_id").agg(
                total_spend=("purchase_amount", "sum"),
                avg_engagement=("engagement_score", "mean")
            ).reset_index()
        )
        self.df = self.df.merge(agg, on="user_id", how="left")
        return self
    def get_data(self):
        return self.df
