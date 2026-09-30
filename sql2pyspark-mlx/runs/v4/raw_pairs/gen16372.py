from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-01-27")
    .groupBy("carrier")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") > 30)
)
