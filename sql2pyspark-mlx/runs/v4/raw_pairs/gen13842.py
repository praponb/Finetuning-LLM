from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

base = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("attr_key").isNotNull())
    .select(
        "*",
        F.upper("attr_key").alias("attr_key_upper"),
        F.upper("attr_key").alias("attr_key_upper_2"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_clean"),
    )
)

base_carts = (
    base
    .filter((F.col("created_at") >= F.lit("2025-03-02 12:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .withColumn("created_at_date", F.to_date("created_at"))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

result = base_carts.select("id", "attr_key_upper_2", "attr_key_clean", "created_at_date")
