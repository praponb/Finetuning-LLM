from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["UK", "DE", "US"]) & (F.col("city") != "Bangkok"))
    .groupBy("city")
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .orderBy("rating_range")
)
