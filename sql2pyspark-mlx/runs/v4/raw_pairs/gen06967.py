from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

result = (
    lines
    .groupBy("tag", "customer_id")
    .agg(
        F.count(F.when(F.col("tag") == "BUNDLE", 1)).alias("bundle_count"),
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.size(F.collect_set("tag")).alias("num_tags_2"),
    )
)
