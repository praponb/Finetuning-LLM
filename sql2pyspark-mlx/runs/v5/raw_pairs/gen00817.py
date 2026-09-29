from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

base = (
    flattened
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 11:00:00"))) & F.col("value").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

base_carts = (
    base
    .filter(F.col("created_at") >= F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "value",
        "days_since_created_at",
        F.ceil("days_since_created_at").alias("days_since_created_at_whole"),
        F.when(F.col("days_since_created_at") >= 62, "High").when(F.col("days_since_created_at") >= 26, "Medium").otherwise("Low").alias("days_since_created_at_level"),
    )
)

joined = (
    base_carts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.name"), F.col("d.segment"))
)

joined_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.days_since_created_at",
        F.col("p.segment").alias("customer_segment"),
        F.col("p.signup_date"),
    )
)

result = (
    joined_carts
    .withColumn("days_since_created_at_tier", F.when(F.col("days_since_created_at") >= 76, "PREMIUM").otherwise("STANDARD"))
    .rollup("days_since_created_at_tier")
    .agg(
        F.count_if(F.col("customer_segment") == "Corporate").alias("corporate_events"),
        F.percentile_approx("days_since_created_at", 0.9).alias("days_since_created_at_p90"),
        F.sum(F.when(F.col("customer_segment") == "SMB", F.col("days_since_created_at")).otherwise(0)).alias("smb_days_since_created_at"),
        F.max("created_at").alias("last_seen"),
        F.round(F.sum("days_since_created_at") / F.nullif(F.count(F.when(F.col("customer_segment") == "SMB", 1)), F.lit(0)), 2).alias("days_since_created_at_per_smb"),
        F.round(F.sum("days_since_created_at") / F.nullif(F.count(F.when(F.col("customer_segment") == "Corporate", 1)), F.lit(0)), 2).alias("days_since_created_at_per_corporate"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("days_since_created_at_per_corporate").asc_nulls_last())
)
