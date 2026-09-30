from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.countDistinct("age").alias("unique_segments"))
    .filter(F.col("unique_segments") < 8)
    .select("segment", "signup_month", "unique_segments")
    .orderBy(F.col("unique_segments").desc())
)
