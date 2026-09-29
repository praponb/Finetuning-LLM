from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-09-24")))
    .select(
        "id",
        "country",
        "city",
        "is_active",
        "credit_limit",
        "age",
        F.greatest("age", "credit_limit").alias("greatest_age_credit_limit"),
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date").between("2024-04-18", "2024-11-15")), "left_semi")
)

not_flagged_customers = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.year(F.col("x.opened_date")) == 2023), "left_anti")
)

not_flagged_customers_2 = (
    not_flagged_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.device") == "mobile"), "left_anti")
)

eligible = (
    not_flagged_customers_2
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.amount") < 480), "left_anti")
)

totals = (
    eligible
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("age")).otherwise(0)).alias("th_age"),
        F.stddev_samp("age").alias("age_stddev"),
        F.round(F.sum("days_since_signup_date") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("days_since_signup_date_per_new_york"),
        F.percentile_approx("days_since_signup_date", [0.25, 0.5, 0.75]).alias("days_since_signup_date_quartiles"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
    )
)

result = (
    totals
    .select("country", "days_since_signup_date_per_new_york")
    .orderBy("country")
    .limit(5)
)
