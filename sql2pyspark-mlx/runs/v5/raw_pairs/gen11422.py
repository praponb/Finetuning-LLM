from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

result = (
    flattened
    .groupBy("customer_id")
    .agg(
        F.count_if(F.col("tag") == "WELCOME5").alias("welcome5_events"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy("welcome5_events")
)
