from pyspark.sql import functions as F

not_flagged = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") < F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("credit_limit") >= 17900, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
        "country",
        F.when(F.col("age") >= 65, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.count("*").alias("event_count"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.avg(F.when(F.col("segment") == "Corporate", F.col("age"))).alias("corporate_avg_age"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
    )
)
