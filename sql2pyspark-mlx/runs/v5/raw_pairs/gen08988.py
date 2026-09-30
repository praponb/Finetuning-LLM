from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-04-05"))) & F.col("channel").isNotNull())
    .withColumn("channel_upper", F.upper("channel"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    filtered
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .cube(F.col("d.region").alias("region"), F.col("a.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.amount")), 2).alias("total_amount"),
        F.grouping_id().alias("gid"),
    )
)
