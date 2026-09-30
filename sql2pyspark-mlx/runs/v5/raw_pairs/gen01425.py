from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Bangkok", "Berlin"])
    .agg(F.max("age"))
    .select(
        "country",
        F.col("Bangkok").alias("bangkok"),
        F.col("Berlin").alias("berlin"),
    )
)
