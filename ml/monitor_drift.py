import pandas as pd
import psycopg2
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from evidently.metrics import ColumnDriftMetric

def get_reference_data():
    # Mock reference data (what the model was trained on)
    return pd.DataFrame({
        "avg_quantity": [2.0, 5.0, 1.0, 15.0, 20.0, 8.0, 12.0],
        "hour_of_day": [10, 14, 9, 18, 19, 12, 15]
    })

def get_current_data():
    # Fetch live data from Postgres (written by Flink)
    conn = psycopg2.connect(
        host="127.0.0.1", port="5433", dbname="kinetix_db", 
        user="postgres", password="supersecret"
    )
    query = """
        SELECT avg_quantity, EXTRACT(HOUR FROM window_start) as hour_of_day
        FROM product_stats
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def main():
    print("🔍 Fetching reference and current data...")
    reference_data = get_reference_data()
    current_data = get_current_data()
    
    if current_data.empty:
        print("⚠️ No live data in product_stats yet. Insert some orders first!")
        return

    print(f"✅ Analyzing drift for {len(current_data)} live records...")
    
    # Generate Evidently Drift Report
    report = Report(metrics=[
        DataDriftPreset(),
        ColumnDriftMetric(column_name="avg_quantity")
    ])
    
    report.run(reference_data=reference_data, current_data=current_data)
    
    # Save as HTML for easy viewing
    report.save_html("drift_report.html")
    print("✅ Drift report saved to drift_report.html! Open it in your browser.")
    
    # Print a quick summary
    as_dict = report.as_dict()
    dataset_drift = as_dict["metrics"][0]["result"]["dataset_drift"]
    print(f"🚨 Dataset Drift Detected: {dataset_drift}")

if __name__ == "__main__":
    main()