from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

valid = (
    lines
    .filter(F.col("code").isNotNull())
    .withColumn("created_at_date", F.to_date("created_at"))
    .withColumn("code_clean", F.coalesce("code", F.lit("UNKNOWN")))
)

filtered = (
    valid
    .filter((F.col("created_at") > F.lit("2025-03-01 21:00:00").cast("timestamp")) & F.col("code").isNotNull())
    .select(
        "*",
        F.add_months("created_at_date", 6).alias("created_at_date_plus_months"),
        F.coalesce("code", F.lit("OTHER")).alias("code_filled"),
    )
)

valid_carts = (
    filtered
    .filter(F.col("created_at_date_plus_months").between("2024-04-18", "2024-11-12") & F.col("code").isNotNull())
    .select("*", F.dayofmonth("created_at").alias("created_at_day"))
)

result = (
    valid_carts
    .select(
        "id",
        "created_at_date",
        "code_clean",
        "created_at_date_plus_months",
        "code_filled",
        "created_at_day",
    )
    .orderBy(F.col("created_at_date").desc_nulls_first(), "created_at_day")
)
