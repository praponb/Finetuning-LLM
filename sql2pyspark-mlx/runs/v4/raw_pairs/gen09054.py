from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([2, 9, 10]))
    .groupBy(
        F.when(F.col("cost") >= 80, "high").when(F.col("cost") >= 19, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count("cost").alias("cost_count"))
    .orderBy(F.col("cost_bucket").desc())
)
