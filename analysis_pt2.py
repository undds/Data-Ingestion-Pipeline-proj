import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import logging

from db.init_db import init_db
from db.connection import connect_to_db

from warnings import filterwarnings

# Suppress only the specific pandas SQL warning
# This is to make the terminal output cleaner
filterwarnings(
    "ignore",
    category=UserWarning,
    message=".*pandas only supports SQLAlchemy connectable.*",
)

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
    init_db(reset=False) 
    conn = connect_to_db()
    cur = conn.cursor()
    try:
        # TODO: two feature engineering examples //  two more visualizations
        
        df = pd.read_sql("SELECT * FROM measurements;", conn)
        pm_query ="""
        SELECT 
    m.unique_id,
    m.indicator_id,
    m.geo_join_id,
    g.geo_place_name,
    m.start_date,
    m.data_value
FROM measurements m
LEFT JOIN geographic g
ON m.geo_join_id = g.geo_join_id
WHERE m.indicator_id = 365;
"""
        print("pm_query type:", type(pm_query))
        df_pm = pd.read_sql_query(pm_query, conn)
        df_pm["start_date"] = pd.to_datetime(df_pm["start_date"], errors="coerce")
        df_pm["data_value"] = pd.to_numeric(df_pm["data_value"], errors="coerce")
        df_pm = df_pm.dropna(subset=["start_date", "data_value", "geo_place_name"]) 
        logging.info(f"Loaded DataFrame with shape {df_pm.shape}")

        # correlate season with data_value where indicator id = 365 (pm2.5)
        df = df[df['indicator_id'] == 365]
        # Convert start_date to datetime and extract the month
        df["start_date"] = pd.to_datetime(df["start_date"])
        df["month"] = df["start_date"].dt.month

        # Create a numeric 'season_idx' column for correlation
        df["season_idx"] = df["month"].apply(get_season)
        #GEO LOCATION
        df_pm["month"] = df["start_date"].dt.month
        df_pm["season_idx"] = df["month"].apply(get_season)
        location_avg = df_pm.groupby("geo_place_name")["data_value"].mean()

        df_pm["location_avg_pollution"] = df_pm["geo_place_name"].map(location_avg)

        df_pm["pollution_deviation"] = (
        df_pm["data_value"] - df_pm["location_avg_pollution"]
)
        # Calculate correlation
        season_corr = df['season_idx'].corr(df['data_value'])
        print(f"Correlation between Season and Air Quality: {season_corr:.2f}")

        # Visualization
        plt.figure(figsize=(10, 6))
        sns.boxplot(x='season_idx', y='data_value', data=df)
        plt.xticks([0, 1], ['Winter', 'Summer'])
        plt.title('PM 2.5 by Season')
        plt.xlabel('Season')
        plt.ylabel('Air Quality Value (PM 2.5)')
        plt.savefig('logs/seasonal_correlation.png')
        plt.show()

        # FEATURE ENGINEERING: ONE-HOT ENCODING
        # converts categorical variables, in this case the indicator name (PM2.5, Ozone, NOx, etc.) into a format that can be provided to ML algorithms to do a better job in prediction.

        # join our measurements table with the indicators table
        query = """
            SELECT 
                m.*, 
                i.name 
            FROM measurements m
            JOIN indicators i ON m.indicator_id = i.indicator_id;
        """

        # read joined data into a df and one-hot encode the indicator name
        df = pd.read_sql(query, conn)
        df_encoded = pd.get_dummies(df, columns=["name"], drop_first=True, dtype=int)
        df_encoded.to_csv("logs/encoded_measurements.csv", index=False)

        print("Data successfully exported to encoded_measurements.csv")

        # FEATURE ENGINEERING: FEATURE SPLITTING
        # split the start_date column into three separate columns: year, month, and day.
        df = pd.read_sql("SELECT * FROM measurements;", conn)
        df["start_date"] = pd.to_datetime(df["start_date"])
        df["year"] = df["start_date"].dt.year
        df["month"] = df["start_date"].dt.month
        df["day"] = df["start_date"].dt.day
        df.to_csv("logs/split_measurements.csv", index=False)
        print("Data successfully exported to split_measurements.csv")


        
        # plot avg pollution plot
        top_n = 10
        top_locations = (
        df_pm.groupby("geo_place_name")["location_avg_pollution"]
        .mean()
        .sort_values(ascending=False)
        .head(top_n)
)

        plt.figure(figsize=(10, 6))
        top_locations.plot(kind="bar")
        plt.title(f"Top {top_n} Locations by Average Pollution (Baseline)")
        plt.xlabel("geo_place_name")
        plt.ylabel("Avg Pollution (data_value)")
        plt.tight_layout()
        plt.savefig("logs/top_locations_avg_pollution.png")
        plt.show()

        #plot deviation 
        plt.figure(figsize=(10,6))
        sns.histplot(df_pm["pollution_deviation"], bins=50, kde=True)
        plt.title("Pollution Deviation From Location Baseline")
        plt.xlabel("Deviation Value")
        plt.ylabel("Frequency")
        plt.tight_layout()
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
