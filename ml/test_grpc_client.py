import grpc
import bentoml

# Connect to the local BentoML gRPC server
with bentoml.SyncHTTPClient("http://localhost:3000") as client:
    print("📡 Sending gRPC prediction request...")
    
    # Call the API we defined in bento_service.py
    response = client.predict_demand(
        avg_quantity=15.5,
        hour_of_day=14  # 2 PM
    )
    
    print("✅ gRPC Response received:")
    print(f"   High Demand: {response['high_demand']}")
    print(f"   Confidence:  {response['confidence']:.2%}")
    print(f"   Price Multiplier: {response['recommended_price_multiplier']}x")