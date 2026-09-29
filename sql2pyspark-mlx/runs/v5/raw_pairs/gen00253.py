from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("discount") <= 0.11)
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
        (F.col("discount") - F.col("amount")).alias("discount_minus_amount"),
    )
)

result = (
    base
    .groupBy(
        "status",
        F.when(F.col("shipping_fee_int") >= 64, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_int_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("shipping_fee_int", 0.5).alias("shipping_fee_int_p50"),
    )
)
