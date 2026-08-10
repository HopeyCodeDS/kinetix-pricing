import bentoml
import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient

# 1. Point MLflow to our local tracking server
mlflow.set_tracking_uri("http://127.0.0.1:5000")

# 2. Define the BentoML Service
@bentoml.service(
    name="pricing_model_service",
    resources={"cpu": "1"},
    traffic={"timeout": 10}
)
class PricingModelService:
    def __init__(self) -> None:
        print("📦 Locating and loading model from MLflow...")
        
        client = MlflowClient()
        experiment_name = "kinetix_pricing_model"
        experiment = client.get_experiment_by_name(experiment_name)
        
        if not experiment:
            raise ValueError(f"Experiment '{experiment_name}' not found. Run the training script first!")
        
        # Find the most recent successful run in this experiment
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=1
        )
        
        if not runs:
            raise ValueError(f"No runs found in experiment '{experiment_name}'.")
            
        latest_run_id = runs[0].info.run_id
        print(f"✅ Found latest run: {latest_run_id}")
        
        # Load the model directly from the run's artifact path
        # (We named the artifact "model" in the training script: mlflow.xgboost.log_model(model, "model"))
        model_uri = f"runs:/{latest_run_id}/model"
        self.model = mlflow.pyfunc.load_model(model_uri)
        print("✅ Model loaded successfully into BentoML!")

    # 3. Define the API endpoint
    @bentoml.api
    def predict_demand(self, avg_quantity: float, hour_of_day: int) -> dict:
        """
        Predicts if there is high demand based on real-time features.
        """
        print(f"🔮 Received prediction request: avg_quantity={avg_quantity}, hour={hour_of_day}")
        
        # Format input for the model 
        input_data = pd.DataFrame([[avg_quantity, hour_of_day]], columns=["avg_quantity", "hour_of_day"])
        
        # Predict
        prediction = int(self.model.predict(input_data)[0])
        
        return {
            "high_demand": bool(prediction),
            "confidence": 0.95 if prediction else 0.80, # Simplified for robustness
            "recommended_price_multiplier": 1.2 if prediction else 1.0
        }
    
    # 4. The Magic: Stateful Windowed Aggregation with Richer Features
    print('⚙️ Executing Stream Processing Pipeline (10-sec tumbling window)...')
    result_table = table_env.sql_query('''
        SELECT 
            product_id,
            AVG(quantity) as avg_quantity,
            COUNT(order_id) as order_velocity,
            SUM(total_amount) as real_time_revenue,
            TUMBLE_START(proc_time, INTERVAL '10' SECOND) as window_start,
            TUMBLE_END(proc_time, INTERVAL '10' SECOND) as window_end
        FROM kafka_orders
        GROUP BY 
            product_id, 
            TUMBLE(proc_time, INTERVAL '10' SECOND)
    ''')



    