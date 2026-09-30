from pyspark.sql import functions as F

src = (
    df_customers
    .filter(~((F.length("email") < 12) & (F.col("credit_limit") > 15000)))
    .withColumn("signup_date_label", F.date_format("signup_date", "yyyy-MM-dd"))
)

segment_summary = (
    src
    .groupBy("segment", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.max("signup_date").alias("last_signup_date"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)

result = (
    segment_summary
    .filter(F.col("max_credit_limit") >= 11500)
    .select("segment", "signup_month", "max_credit_limit")
)
