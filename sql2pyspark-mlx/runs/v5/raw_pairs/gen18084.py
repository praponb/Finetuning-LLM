from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy("key")
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.countDistinct("key").alias("distinct_keys"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy("key")
)
