from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "London"])
    .agg(F.sum("age"))
    .select(
        "country",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("London").alias("london"),
    )
)
