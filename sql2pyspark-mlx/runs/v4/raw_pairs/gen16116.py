from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)

result = (
    valid
    .groupBy("customer_id", "store_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.percentile_approx("amount_with_shipping", 0.9).alias("amount_with_shipping_p90"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy("cnt")
)
