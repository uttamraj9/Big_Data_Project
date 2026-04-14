from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql import Row

# Start Spark session
spark = SparkSession.builder.appName("FraudPredictionService").getOrCreate()

# Load the trained model
model = PipelineModel.load("randomforest_pipeline_model")

# List of features used in training (same order)
feature_cols = ['transaction_amount', 'account_balance', 'card_age', 'ip_address_flag',
                'avg_transaction_amount_7d', 'risk_score', 'is_weekend', 'risk_score',
                'previous_fraudulent_activity', 'daily_transaction_count', 'transaction_distance',
                'device_type_vec', 'merchant_category_vec', 'card_type_vec', 'location_vec',
                'authentication_method_vec', 'transaction_type_vec']

app = Flask(__name__)

@app.route('/')
def home():
    return "API is running. Use POST /predict to get predictions."

@app.route('/predict', methods=['POST'])
def predict():
    json_data = request.get_json()
    features = json_data.get("features")

    if not features or len(features) != len(feature_cols):
        return jsonify({"error": "Invalid or missing features"}), 400

    # Create DataFrame
    df = spark.createDataFrame([Row(**dict(zip(feature_cols, features)))])
    prediction = model.transform(df).select("prediction").collect()[0][0]

    return jsonify({"prediction": float(prediction)})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
