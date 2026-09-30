from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%oky%"))
    .withColumn("supplier_name_title", F.initcap(F.trim("supplier_name")))
    .withColumn("rating_whole", F.ceil("rating"))
    .withColumn("city_filled", F.coalesce("city", F.lit("DIRECT")))
)

result = (
    base
    .groupBy("country")
    .pivot("city", ["Berlin", "Chiang Mai", "Tokyo"])
    .agg(F.avg("rating_whole"))
    .select(
        "country",
        F.col("Berlin").alias("berlin"),
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("Tokyo").alias("tokyo"),
    )
)
