from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("corr_age_credit_limit").desc())

matched_keys = (
    df_customers
    .select("segment")
    .intersect(
        df_customers
        .filter(F.col("credit_limit") <= 7400)
        .select("segment")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("credit_limit"))).alias("jp_total_credit_limit"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.stddev_samp("age").alias("age_stddev"),
    )
    .withColumn("corr_age_credit_limit_percent_rank", F.percent_rank().over(w))
)
