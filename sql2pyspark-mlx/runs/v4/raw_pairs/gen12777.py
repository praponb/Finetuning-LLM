from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "Chiang Mai"]) & ((F.col("rating") <= 3.4) | (F.col("city") == "New York")))
)

suppliers_stats = (
    base
    .groupBy(
        F.when(F.col("city").isin(["Chiang Mai", "Singapore"]), "priority").otherwise("other").alias("city_group"),
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg("rating").alias("avg_rating"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("cnt") < 3)
    .orderBy(F.col("avg_rating").desc(), F.col("cnt").desc())
)
