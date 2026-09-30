from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("credit_limit_scaled").desc_nulls_last())
w2 = Window.partitionBy("customer_id")

filtered = (
    df_sessions
    .filter(F.col("country").isNull())
    .withColumn("clean_country", F.trim("country"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("NONE")))
)

joined = (
    filtered
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.email"), F.col("b.credit_limit"))
)

remaining_keys = (
    joined
    .select("browser")
    .intersectAll(
        df_sessions
        .filter(F.col("country").isin(["SG", "US"]))
        .select("browser")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter((F.col("started_at") > F.to_date(F.lit("2023-02-15"))) & (~F.col("browser").eqNullSafe(F.lit("Chrome"))))
    .select(
        "customer_id",
        "browser",
        "country",
        "clean_country",
        "browser_filled",
        "email",
        F.round((F.col("credit_limit") * 1.07), 2).alias("credit_limit_scaled"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

base_sessions = (
    base
    .filter(F.col("country").isNotNull())
    .select(
        "customer_id",
        "country",
        "clean_country",
        "browser_filled",
        "email",
        "credit_limit_scaled",
        "country_clean",
        F.lower("country").alias("country_lower"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

filtered_sessions = (
    base_sessions
    .filter(F.col("customer_id").isNotNull() & (~F.col("country").eqNullSafe(F.lit("JP"))))
    .withColumn("email_country_key", F.concat_ws(",", "email", "country"))
    .withColumn("country_filled", F.coalesce("country", F.lit("NONE")))
)

enriched = (
    filtered_sessions
    .select(
        "customer_id",
        "browser_filled",
        "country_clean",
        F.percent_rank().over(w).alias("credit_limit_scaled_percent_rank"),
        F.stddev_samp("credit_limit_scaled").over(w2).alias("customer_id_stddev_samp_credit_limit_scaled"),
    )
)

result = (
    enriched
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.segment").alias("segment"),
        F.col("dim.country").alias("country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.customer_id_stddev_samp_credit_limit_scaled")).alias("min_customer_id_stddev_samp_credit_limit_scaled"),
    )
)
