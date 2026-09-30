from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("greatest_age_is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("greatest_age_is_active_flag", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("city").isNotNull() & F.col("city").isNull())
    .select(
        "*",
        F.regexp_replace("email", " ", "").alias("email_slug"),
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

filtered = (
    base
    .filter((~F.col("city").eqNullSafe(F.lit("London"))) & F.col("name").like("%Kim%"))
    .withColumn("greatest_age_is_active_flag", F.greatest("age", "is_active_flag"))
)

enriched = (
    filtered
    .select(
        "*",
        F.first("is_active_flag", ignorenulls=True).over(w).alias("first_known_is_active_flag"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_3"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.city",
        "s.age",
        "s.first_known_is_active_flag",
        "s.credit_limit_avg_3",
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
        F.col("c.end_date"),
    )
)

result = (
    latest_match
    .groupBy(F.date_trunc("week", "end_date").alias("week_start"))
    .agg(
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.round(F.sum("credit_limit_avg_3") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("credit_limit_avg_3_per_bangkok"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("age")).otherwise(0)).alias("bangkok_age"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
)
