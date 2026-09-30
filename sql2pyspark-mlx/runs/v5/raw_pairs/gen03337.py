from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.size("items").alias("num_items"),
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
    )
)

result = (
    unpacked
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.min("created_at").alias("first_created_at"),
        F.round(F.avg("num_items"), 2).alias("avg_num_items"),
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
        F.min("created_at").alias("first_created_at_2"),
    )
    .orderBy(F.col("avg_num_items").desc_nulls_last())
)
