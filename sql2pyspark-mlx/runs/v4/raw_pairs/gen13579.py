from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

enriched = (
    exploded
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email"), F.col("p.name"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.city"), F.col("b.segment"))
)

valid = (
    with_dims
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("k").isNotNull() & (F.col("segment") == "SMB"))
    .select(
        "id",
        "created_at",
        "k",
        "v",
        "email",
        "city",
        "segment",
        F.substring("email", 1, 4).alias("email_prefix"),
        F.to_date("created_at").alias("created_at_date"),
    )
)

filtered = (
    valid
    .filter(F.col("segment") == "Consumer")
    .select(
        "id",
        "created_at",
        "k",
        "v",
        "email",
        "segment",
        "email_prefix",
        "created_at_date",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
        F.last_day("created_at_date").alias("created_at_date_month_end"),
    )
)

result = (
    filtered
    .select("created_at", "k", "v")
    .orderBy(F.col("k").desc_nulls_first(), "created_at")
)
