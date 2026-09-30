from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "London"])
    .agg(F.avg("rating").alias("value_sum"))
    .select(
        "country",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("London").alias("london"),
    )
)
