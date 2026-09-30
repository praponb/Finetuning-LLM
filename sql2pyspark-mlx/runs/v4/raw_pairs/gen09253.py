from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("age").desc_nulls_last(), "id")
w2 = Window.partitionBy("segment").orderBy("age")
w3 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2024-01-21"))) & F.col("city").isNotNull() & F.lower("name").like("%lic%"))
    .select(
        "*",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.initcap(F.trim("country")).alias("country_title"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") >= F.col("s.signup_date")), "left_anti")
)

latest = (
    kept
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.percent_rank().over(w2).alias("age_percent_rank"),
        F.percent_rank().over(w3).alias("credit_limit_percent_rank"),
        F.first("segment", ignorenulls=True).over(w4).alias("first_known_segment"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("age") >= 57, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
    )
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.round(F.avg("credit_limit_percent_rank"), 2).alias("avg_credit_limit_percent_rank"),
        F.avg("credit_limit_percent_rank").alias("avg_credit_limit_percent_rank_2"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .filter(F.col("corr_credit_limit_age") > 0)
    .orderBy("corr_credit_limit_age")
)
