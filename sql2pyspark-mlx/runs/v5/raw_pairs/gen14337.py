from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-06").cast("date")) & (F.col("amount") > 1290))
    .select("*", F.date_sub("order_date", 23).alias("order_date_minus_23d"))
)

result = (
    cleaned
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .groupingSets([["status", "quarter_start"], ["status"]], "status", "quarter_start")
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("amount_per_completed"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("avg_shipping_fee").desc())
)
