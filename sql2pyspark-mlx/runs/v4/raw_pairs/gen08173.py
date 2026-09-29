from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Singapore", "Tokyo", "Chiang Mai"])
    .agg(F.avg("rating").alias("amount"))
    .select(
        "country",
        F.col("Singapore").alias("singapore"),
        F.col("Tokyo").alias("tokyo"),
        F.col("Chiang Mai").alias("chiang_mai"),
    )
)
