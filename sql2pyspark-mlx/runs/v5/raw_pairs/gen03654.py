from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-08-09"))) & F.col("customer_id").isin([7, 15, 26]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.upper("channel").alias("channel_upper"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)

result = (
    filtered
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.max("amount_int").alias("max_amount_int"),
        F.sum(F.when(F.col("channel") == "web", F.col("amount_int")).otherwise(0)).alias("web_amount_int"),
    )
    .orderBy(F.col("web_amount_int").desc_nulls_last())
)
