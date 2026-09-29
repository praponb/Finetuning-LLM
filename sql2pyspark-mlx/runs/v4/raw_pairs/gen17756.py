from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "UK") & F.lower("supplier_name").like("%uro%"))
    .groupBy("country", "city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.countDistinct("rating").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") < 4)
    .orderBy(F.col("unique_citys").desc(), "country")
)
