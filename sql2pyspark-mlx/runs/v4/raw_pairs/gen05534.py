from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "country")
    .pivot("segment", ["Consumer", "Corporate"])
    .agg(F.max("credit_limit").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "city",
        "country",
        F.col("Consumer_amount").alias("consumer_amount"),
        F.col("Consumer_cnt").alias("consumer_cnt"),
        F.col("Corporate_amount").alias("corporate_amount"),
        F.col("Corporate_cnt").alias("corporate_cnt"),
    )
)
