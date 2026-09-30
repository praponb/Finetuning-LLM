from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("days_since_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-03-25").cast("date")) & (F.lower(F.trim("name")) == "carol diaz"))
    .select(
        "id",
        "name",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.sum("days_since_signup_date").over(w).alias("cumulative_days_since_signup_date"),
        F.first("age", ignorenulls=True).over(w2).alias("first_known_age"),
        F.avg("age").over(w3).alias("age_avg_6_prior"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.balance").alias("account_balance"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("cumulative_days_since_signup_date") >= 360, "HIGH").otherwise("LOW").alias("cumulative_days_since_signup_date_tier"),
    )
    .agg(
        F.max(F.when(F.col("segment") == "Consumer", F.col("first_known_age"))).alias("consumer_max_first_known_age"),
        F.percentile_approx("first_known_age", [0.25, 0.5, 0.75]).alias("first_known_age_quartiles"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
        F.sum("account_balance").alias("total_account_balance"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("total_account_balance") < 0)
    .orderBy(F.col("total_account_balance").desc())
)
