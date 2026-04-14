from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.linalg import Vector

vector_to_array_udf = udf(
    lambda v: v.toArray().tolist() if v is not None else None,
    ArrayType(DoubleType())
)

vec_cols = [
    "transaction_type_Vec",
    "device_type_Vec",
    "location_Vec",
    "merchant_category_Vec",
    "card_type_Vec",
    "authentication_method_Vec"
]

for vec in vec_cols:
    arr_col = vec + "_arr"

    # a) Vector -> Array[Double]
    df = df.withColumn(arr_col, vector_to_array_udf(col(vec)))

    # b) Get the length from the first row’s array
    first_arr = df.select(arr_col).head()[0] or []
    size = len(first_arr)

    # c) Spill each element into its own Double column
    for i in range(size):
        new_col = "{}_{}".format(vec, i)
        df = df.withColumn(new_col, col(arr_col)[i])

    # d) Drop the old vector and helper array
    df = df.drop(vec, arr_col)


df.write.mode("overwrite").parquet("/tmp/US_UK_05052025/class_project/input/ml_data/parquet_flattened")

