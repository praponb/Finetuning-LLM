from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & ~F.col("city").isin(["Singapore", "New York"]) & (F.col("rating") < 2.5))
    .groupBy("country")
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("stddev_rating") >= 59)
    .orderBy(F.col("rating_count").desc(), "country")
    .limit(1)
)
