from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") > 60) & (F.col("order_id").isin([14, 18, 35]) | (F.col("delivered_date") > "2024-06-01")))
    .groupBy(
        F.when(F.col("cost") >= 103, "high").when(F.col("cost") >= 25, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.max("delivered_date").alias("latest_delivered_date"))
    .orderBy("latest_delivered_date")
)
