from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-04-10").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "discount",
        "shipping_fee",
        F.length("status").alias("status_length"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

result = (
    valid
    .groupBy("status")
    .agg(
        F.count("*").alias("row_count"),
        F.corr("discount", "shipping_fee").alias("corr_discount_shipping_fee"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg(F.when(F.col("status") == "SHIPPED", F.col("shipping_fee"))).alias("shipped_avg_shipping_fee"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .filter(F.col("avg_discount") >= 0)
    .orderBy(F.col("corr_discount_shipping_fee").desc_nulls_first())
)
