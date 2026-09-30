from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("cost_tier", F.when(F.col("cost") >= 78, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("WEEK", "shipped_date"))
    .groupingSets(
        [["cost_tier", "week_start"], ["cost_tier"], []],
        "cost_tier",
        "week_start",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.max(F.when(F.col("carrier") == "Flash", F.col("cost"))).alias("flash_max_cost"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("cost_stddev") >= 0)
    .orderBy(F.col("cost_stddev").desc_nulls_last())
)
