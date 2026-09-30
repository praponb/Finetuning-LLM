from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "country")
    .pivot("city", ["Berlin", "Tokyo", "Chiang Mai"])
    .agg(F.avg("age"))
    .select(
        "segment",
        "country",
        F.col("Berlin").alias("berlin"),
        F.col("Tokyo").alias("tokyo"),
        F.col("Chiang Mai").alias("chiang_mai"),
    )
)
