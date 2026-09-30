from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Chiang Mai", "Bangkok", "London"]))
    .groupBy("country", F.month("signup_date").alias("signup_month"))
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("consumer_count") >= 6)
    .select("country", "signup_month", "consumer_count", "row_count")
    .orderBy("row_count", F.col("consumer_count").desc())
)
