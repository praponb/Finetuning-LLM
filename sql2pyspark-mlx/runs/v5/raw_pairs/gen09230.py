from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%GmbH%"))
    .groupBy("country")
    .pivot("city", ["Tokyo", "Singapore", "Berlin", "New York"])
    .agg(F.max("rating"))
)
