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

base = (
    lines
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("clean_tag", F.trim("tag"))
)

joined = (
    base
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.segment"))
)

cleaned = (
    joined
    .filter(F.col("tag").isNotNull())
    .select(
        "*",
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "customer_id",
        "created_at",
        "tag",
        "segment",
        "is_consumer",
        "segment_clean",
    )
)
