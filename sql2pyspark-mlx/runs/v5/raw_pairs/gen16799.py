from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .groupBy("customer_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
