from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Chiang Mai"]))
    .groupBy("country")
    .agg(
        F.count("rating").alias("rating_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 3)
    .orderBy(F.col("rating_count").desc(), "cnt")
)
