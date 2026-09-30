from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.countDistinct("credit_limit").alias("unique_segments"),
    )
    .orderBy(F.col("avg_credit_limit").desc(), F.col("signup_month").desc())
)
