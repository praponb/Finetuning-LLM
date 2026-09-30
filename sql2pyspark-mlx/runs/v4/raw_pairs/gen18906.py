from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Sup%"))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("num_suppliers"),
        F.countDistinct("rating").alias("unique_citys"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy("num_suppliers")
)
