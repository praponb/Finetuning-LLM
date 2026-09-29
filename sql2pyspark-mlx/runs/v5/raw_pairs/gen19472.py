from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

flattened_carts = (
    flattened
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_value",
        F.explode("coupon_codes").alias("element"),
    )
)

filtered = (
    flattened_carts
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("attr_key").isNotNull())
    .withColumn("created_at_date", F.to_date("created_at"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("DIRECT")))
)

base = (
    filtered
    .filter(F.col("attr_key").isNotNull())
    .select(
        "created_at",
        "attr_key",
        "attr_value",
        "created_at_date",
        "attr_key_filled",
        F.dayofmonth("created_at").alias("created_at_day"),
    )
)

result = (
    base
    .select(
        "attr_key",
        "attr_value",
        "created_at_date",
        "attr_key_filled",
        "created_at_day",
    )
    .orderBy(F.col("attr_key").desc())
)
