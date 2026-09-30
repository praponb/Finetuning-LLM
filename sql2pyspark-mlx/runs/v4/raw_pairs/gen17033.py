from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 16200, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
        "country",
        F.date_trunc("year", "signup_date").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("credit_limit")).otherwise(0)).alias("jp_credit_limit"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)
