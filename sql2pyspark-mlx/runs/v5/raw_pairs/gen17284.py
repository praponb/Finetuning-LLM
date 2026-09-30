from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w_hist = Window.orderBy(F.col("age").desc_nulls_first())
w3 = Window.partitionBy("account_account_type", "segment")

latest_match = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type").alias("account_account_type"))
)

kept_keys = (
    latest_match
    .select("city")
    .subtract(
        df_customers
        .filter(F.col("segment").isin(["Consumer", "SMB"]))
        .select("city")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

matched_keys = (
    kept_rows
    .select("country", "city")
    .exceptAll(
        df_customers
        .filter(F.col("city") == "Berlin")
        .select("country", "city")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        F.cume_dist().over(w_hist).alias("age_cume_dist"),
        F.corr("credit_limit", "age").over(w3).alias("corr_credit_limit_age"),
        F.dense_rank().over(w_hist).alias("age_dense_rank"),
    )
)

valid = (
    scored
    .filter((F.col("signup_date") > F.lit("2023-08-21").cast("date")) & F.col("city").isNotNull())
    .select(
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "corr_credit_limit_age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.least("age_dense_rank", "corr_credit_limit_age").alias("least_age_dense_rank_corr_credit_limit_age"),
    )
)

filtered = (
    valid
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-03-12")))
    .withColumn("is_de", F.when(F.col("country") == "DE", 1).otherwise(0))
)

base = (
    filtered
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-05-25")))
    .select(
        "country",
        "is_active",
        "corr_credit_limit_age",
        "is_active_label",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)

result = (
    base
    .cube("country")
    .agg(
        F.round(F.sum("corr_credit_limit_age") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("corr_credit_limit_age_per_uk"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.countDistinct("country").alias("distinct_countries"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.round(F.avg("corr_credit_limit_age"), 2).alias("avg_corr_credit_limit_age"),
    )
    .orderBy(F.col("distinct_countries").desc())
)
