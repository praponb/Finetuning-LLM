from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-23").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.add_months("order_date", 12).alias("order_date_plus_months"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    base
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .cube("quarter_start", "status")
    .agg(
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max(F.when(F.col("status") == "CANCELLED", F.col("shipping_fee"))).alias("cancelled_max_shipping_fee"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.grouping("quarter_start").alias("grouping_id"),
    )
    .filter(F.col("cancelled_max_shipping_fee") > 0)
    .orderBy(F.col("quarter_start").desc())
)
