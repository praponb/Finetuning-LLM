from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2024-05-01")
    .groupBy("status")
    .agg(
        F.min("cost").alias("min_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .filter(F.col("min_cost") < 89)
    .orderBy("status")
)
