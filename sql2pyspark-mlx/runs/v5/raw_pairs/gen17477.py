from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

lines = (
    exploded
    .select("id", "created_at", "k", F.explode_outer("coupon_codes").alias("element"))
)

filtered = (
    lines
    .filter(F.col("k").isNotNull() & (F.col("element") == "SPRING10"))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
)

base = (
    filtered
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 23:00:00"))) & (F.col("created_at") >= "2023-11-28"))
    .withColumn("days_since_created_at_rounded", F.round("days_since_created_at", 2))
    .withColumn("element_k", F.concat("element", F.lit("/"), "k"))
)

valid = (
    base
    .filter(F.col("created_at") < "2024-02-01")
    .select(
        "id",
        "created_at",
        "k",
        "element",
        F.round((F.col("days_since_created_at") * 1.15), 2).alias("adjusted_days_since_created_at"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

base_carts = (
    valid
    .filter(F.col("element").isNotNull())
    .select(
        "id",
        "created_at",
        "adjusted_days_since_created_at",
        "resolved_k",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

result = (
    base_carts
    .select("id", "created_at", "adjusted_days_since_created_at", "resolved_k")
    .orderBy(F.col("adjusted_days_since_created_at").desc())
)
