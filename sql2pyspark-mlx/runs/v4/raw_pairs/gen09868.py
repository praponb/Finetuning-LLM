from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("row_count"),
        F.sum("rating").alias("total_rating"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .filter(F.col("avg_rating") > 0)
    .orderBy(F.col("rating_tier").asc_nulls_last())
)
