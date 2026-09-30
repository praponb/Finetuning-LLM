from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("credit_limit").desc_nulls_last(), "id")
w2 = Window.partitionBy("country").orderBy("age")

kept = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.start_date") >= F.col("s.signup_date")), "left_semi")
)

kept_customers = (
    kept
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.month(F.col("ch.opened_date")).isin([1, 6, 8])), "left_anti")
)

latest = (
    kept_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_customers = (
    latest
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

summary = (
    latest_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 17800, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("credit_limit_per_th"),
    )
)

result = (
    summary
    .select(
        "credit_limit_tier",
        "corr_age_credit_limit",
        "credit_limit_p50",
        "credit_limit_per_th",
    )
    .orderBy(F.col("credit_limit_per_th").desc())
)
