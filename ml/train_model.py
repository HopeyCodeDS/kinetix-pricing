import pandas as pd
import psycopg2
import mlflow
import mlflow.xgboost
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

def get_features_from_db():
    """Fetch real-time aggregated features from Postgres (written by Flink)"""
    conn = psycopg2.connect(
        host="127.0.0.1", port="5433", dbname="kinetix_db", 
        user="postgres", password="supersecret"
    )
    query = """
        SELECT product_id, avg_quantity, 
               EXTRACT(HOUR FROM window_start) as hour_of_day
        FROM product_stats
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Create a mock target variable for demonstration 
    # (e.g., is this a "high demand" period? avg_quantity > 10)
    df['high_demand'] = (df['avg_quantity'] > 10).astype(int)
    return df

def main():
    print("🚀 Fetching real-time features from Postgres...")
    df = get_features_from_db()
    
    if df.empty:
        print("⚠️ No data in product_stats yet. Insert some orders and wait 15 seconds!")
        return

    print(f"✅ Loaded {len(df)} feature records.")
    
    X = df[['avg_quantity', 'hour_of_day']]
    y = df['high_demand']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Start MLflow Run
    mlflow.set_tracking_uri("http://127.0.0.1:5000")
    mlflow.set_experiment("kinetix_pricing_model")
    
    with mlflow.start_run(run_name="xgboost_demand_predictor"):
        print("🧠 Training XGBoost Model...")
        model = xgb.XGBClassifier(n_estimators=10, max_depth=3, random_state=42)
        model.fit(X_train, y_train)
        
        # Evaluate
        preds = model.predict(X_test)
        accuracy = accuracy_score(y_test, preds)
        print(f"✅ Model Accuracy: {accuracy:.2f}")
        
        # Log to MLflow
        mlflow.log_param("n_estimators", 10)
        mlflow.log_param("max_depth", 3)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.xgboost.log_model(model, "model")
        
        print("🎯 Model successfully logged to MLflow!")

if __name__ == "__main__":
    main()