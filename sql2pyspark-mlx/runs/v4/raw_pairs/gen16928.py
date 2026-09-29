from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 12900, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("segment").alias("distinct_segments"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
    )
)
