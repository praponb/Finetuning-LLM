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

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.segment"), F.col("d.email"))
)

valid = (
    with_dims
    .filter(F.col("created_at") > F.to_date(F.lit("2025-03-01")))
    .withColumn("segment_prefix", F.substring("segment", 1, 4))
    .withColumn("email_length", F.length("email"))
)

result = valid.select("created_at", "tag", "email", "segment_prefix", "email_length")
