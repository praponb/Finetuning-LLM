from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Berlin", "Bangkok", "Chiang Mai"])
    .agg(F.max("age").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "country",
        F.col("Berlin_amount").alias("berlin_amount"),
        F.col("Berlin_n").alias("berlin_n"),
        F.col("Bangkok_amount").alias("bangkok_amount"),
        F.col("Bangkok_n").alias("bangkok_n"),
        F.col("Chiang Mai_amount").alias("chiang_mai_amount"),
        F.col("Chiang Mai_n").alias("chiang_mai_n"),
    )
)
