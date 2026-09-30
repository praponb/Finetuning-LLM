from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

filtered = (
    flattened
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 13:00:00")))
    .withColumn("created_at_day", F.col("created_at").cast("date"))
    .withColumn("created_at_period", F.date_trunc("week", "created_at"))
    .withColumn("element_filled", F.coalesce("element", F.lit("NONE")))
)

filtered_carts = (
    filtered
    .filter(F.col("element") == "VIP")
    .select(
        "id",
        "created_at",
        "created_at_day",
        "created_at_period",
        "element_filled",
        F.to_date("created_at").alias("created_at_date"),
    )
)

base = (
    filtered_carts
    .filter(F.to_date("created_at") >= "2024-11-01")
    .select(
        "created_at_day",
        "created_at_period",
        "element_filled",
        F.add_months("created_at_date", 6).alias("created_at_date_plus_months"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("YEAR", "created_at_date_plus_months").alias("year_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min("created_at_period").alias("started_at"),
        F.count("*").alias("row_count"),
        F.min("created_at_period").alias("started_at_2"),
        ((F.unix_timestamp(F.max("created_at_period")) - F.unix_timestamp(F.min("created_at_period"))) / 60).alias("span_minutes"),
        F.max("created_at_period").alias("last_seen"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
