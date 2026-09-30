from pyspark.sql import functions as F

remaining_keys = (
    df_shipments
    .select("id")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("week_start", F.date_trunc("WEEK", "delivered_date"))
    .groupingSets([["order_id", "week_start"], ["order_id"]], "order_id", "week_start")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.min("cost").alias("min_cost"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "Kerry", 1)), F.lit(0)), 2).alias("cost_per_kerry"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("min_cost")
)
