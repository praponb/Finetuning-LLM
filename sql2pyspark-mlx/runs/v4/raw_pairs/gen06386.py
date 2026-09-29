from pyspark.sql import functions as F

joined = (
    df_orders
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.opened_date"))
)

result = (
    joined
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.34, "LARGE").otherwise("SMALL"))
    .rollup("discount_tier")
    .agg(
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("num_rows").desc())
)
