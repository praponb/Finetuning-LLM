from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("country").isin(["TH", "US"]))

suppliers_stats = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.avg("rating").alias("avg_rating"),
    )
)

result = suppliers_stats.select("rating_bucket", "unique_citys", "avg_rating")
