from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("city").isNotNull() & F.col("city").isNotNull())
    .select("*", F.initcap(F.trim("name")).alias("name_title"))
)

remaining_keys = (
    base
    .select("city")
    .subtract(
        df_customers
        .filter(F.col("credit_limit").isNotNull())
        .select("city")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") < F.col("s.signup_date")), "left_anti")
)

kept_customers = (
    kept
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.device") != "desktop"), "left_anti")
)

result = (
    kept_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 15300, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
        F.when(F.col("age") >= 70, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.stddev_samp("age").alias("age_stddev"),
        F.min("age").alias("min_age"),
        F.countDistinct("country").alias("distinct_countries"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
    )
    .filter(F.col("age_stddev") < 1000000)
    .orderBy(F.col("distinct_countries").asc_nulls_last())
)
