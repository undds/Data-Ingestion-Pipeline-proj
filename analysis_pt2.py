print("1")
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import os
import logging

from db.init_db import init_db
from db.connection import connect_to_db


def get_season(month):
    if month in [12, 1, 2]:
        return 1  # Winter
    elif month in [6, 7, 8]:
        return 2  # Summer
    elif month in [9, 10, 11]:
        return 3  # Fall
    else:
        return 4  # Spring

def setup_logging(log_level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        filename="logs/analysis.log",
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True
    )

def main() -> None:
    setup_logging("INFO")
    logging.info("Starting analysis")
    print("1.5")
    init_db(reset=False) 
    print("2")
    conn = connect_to_db()
    cur = conn.cursor()
    print("3")
    try:
        # TODO: Add analysis stuff here
        # two feature engineering examples
        # one correlation
        # three visualizations

        #
        df = pd.read_sql("SELECT * FROM measurements;", conn)
        geo_df = pd.read_sql("SELECT geo_join_id, geo_place_name FROM geographic;", conn)
        df = pd.read_sql("""
SELECT 
    m.unique_id,
    m.indicator_id,
    m.geo_join_id,
    g.geo_place_name,
    m.start_date,
    m.data_value
FROM measurements m
LEFT JOIN geographic g
ON m.geo_join_id = g.geo_join_id;
""", conn)
        logging.info(f"Loaded DataFrame with shape {df.shape}")

        # correlate season with data_value where indicator id = 365 (pm2.5)
        df = df[df['indicator_id'] == 365]
        # Convert start_date to datetime and extract the month
        df["start_date"] = pd.to_datetime(df["start_date"])
        df["month"] = df["start_date"].dt.month

        # Create a numeric 'season_idx' column for correlation
        df["season_idx"] = df["month"].apply(get_season)
        #GEO LOCATION
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df = df.dropna(subset=["start_date", "data_value", "geo_place_name"])

        df["month"] = df["start_date"].dt.month
        df["season_idx"] = df["month"].apply(get_season)
        location_avg = df.groupby("geo_place_name")["data_value"].mean()
        df["location_avg_pollution"] = df["geo_place_name"].map(location_avg)
        df["pollution_deviation"] = df["data_value"] - df["location_avg_pollution"]
        # Calculate correlation
        season_corr = df['season_idx'].corr(df['data_value'])
        print(f"Correlation between Season and Air Quality: {season_corr:.2f}")

        # Visualization (Box Plot is best for categorical correlation)

        plt.figure(figsize=(10, 6))
        sns.boxplot(x='season_idx', y='data_value', data=df)
        plt.xticks([0, 1], ['Winter', 'Summer'])
        plt.title('PM 2.5 by Season')
        plt.xlabel('Season')
        plt.ylabel('Air Quality Value (PM 2.5)')
        plt.savefig('logs/seasonal_correlation.png')
        plt.show()

        # plot avg pollution plot
        top_n = 10
        top_locations = (
        df.groupby("geo_place_name")["location_avg_pollution"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
)

        plt.figure(figsize=(10, 6))
        top_locations.plot(kind="bar")
        plt.title(f"Top {top_n} Locations by Average Pollution (Baseline)")
        plt.xlabel("geo_join_id")
        plt.ylabel("Avg Pollution (data_value)")
        plt.tight_layout()
        plt.savefig("logs/top_locations_avg_pollution.png")
        plt.show()

        #plot deviation 
        plt.figure(figsize=(10,6))
        sns.histplot(df["pollution_deviation"], bins=50, kde=True)
        plt.title("Pollution Deviation From Location Baseline")
        plt.xlabel("Deviation Value")
        plt.ylabel("Frequency")
        plt.savefig("logs/pollution_deviation.png")
        plt.show()

    except Exception as e:
        conn.rollback()
        logging.error(f"Database connection failed during analysis: {e}")
        raise

    # close connections
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
