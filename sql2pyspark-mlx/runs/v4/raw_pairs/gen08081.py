from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 4.1)
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "Chiang Mai"]), "core").otherwise("other").alias("city_group"),
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy("city_group")
)
