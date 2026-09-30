from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

joined = (
    lines
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.country"), F.col("b.segment"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.segment").alias("customer_segment"), F.col("c.city"))
)

result = (
    with_dims
    .select(
        "created_at",
        "code",
        "segment",
        "customer_segment",
        "city",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc())
)
