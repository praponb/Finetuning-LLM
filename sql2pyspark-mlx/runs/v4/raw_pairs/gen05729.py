from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.month("shipped_date").isin([4, 7, 9]) & (F.col("cost") >= 94))
    .withColumn("delivered_date_minus_87d", F.date_sub("delivered_date", 87))
)

cost_bucket_summary = (
    recent_shipments
    .groupBy(
        F.when(F.col("cost") >= 97, "high").when(F.col("cost") >= 22, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = (
    cost_bucket_summary
    .filter(F.col("avg_cost") > 61)
    .orderBy(F.col("avg_cost").desc())
)
