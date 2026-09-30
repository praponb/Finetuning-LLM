from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") >= 6)
    .select("start_year", "monthly_fee_count")
    .orderBy(F.col("monthly_fee_count").desc(), "start_year")
    .limit(5)
)
