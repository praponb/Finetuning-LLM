from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

result = (
    lines
    .groupBy("customer_id", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.count_if(F.col("element") == "BUNDLE").alias("bundle_events"),
        F.size(F.collect_set("element")).alias("num_elements"),
    )
    .filter(F.col("num_elements") >= 1)
    .orderBy(F.col("bundle_events").desc_nulls_first())
)
