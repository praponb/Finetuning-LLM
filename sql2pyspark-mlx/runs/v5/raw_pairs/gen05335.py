from pyspark.sql import functions as F

kept = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
    )
    .filter(F.col("de_events") > 1)
)
