from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .groupBy("attr_key")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
    )
)
