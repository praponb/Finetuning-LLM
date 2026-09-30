from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Berlin"]) & F.col("city").isNotNull())
)

agg = (
    filtered_suppliers
    .groupBy("country")
    .agg(F.avg("rating").alias("avg_rating"))
)

result = (
    agg
    .filter(F.col("avg_rating") >= 3.9)
    .orderBy("country")
)
