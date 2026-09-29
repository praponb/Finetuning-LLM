from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("delivered_date") < "2023-11-05") & (F.col("order_id") == 36)) | (F.col("cost") < 65))
    .groupBy(
        F.when(F.col("cost") >= 87, "high").when(F.col("cost") >= 33, "mid").otherwise("low").alias("cost_bucket"),
        "status",
    )
    .agg(F.min("shipped_date").alias("min_shipped_date"))
)
