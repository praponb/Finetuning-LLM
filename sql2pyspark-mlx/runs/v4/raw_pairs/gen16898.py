from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("city_length", F.length("city"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("N/A")))
)

result = (
    cleaned
    .groupBy("country")
    .pivot("city", ["New York", "Bangkok"])
    .agg(F.sum("rating"))
    .select(
        "country",
        F.col("New York").alias("new_york"),
        F.col("Bangkok").alias("bangkok"),
    )
)
