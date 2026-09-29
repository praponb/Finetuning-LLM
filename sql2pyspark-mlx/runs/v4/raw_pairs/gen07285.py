from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("year", "signup_date").alias("year_start"))
    .agg(
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_rows"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
    )
    .filter(F.col("de_rows") < 1)
    .orderBy("year_start")
)
