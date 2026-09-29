from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 104, "high").when(F.col("cost") >= 43, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.max("shipped_date").alias("latest_shipped_date"))
    .orderBy("cost_bucket", "latest_shipped_date")
)
