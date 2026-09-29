from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.sum("rating").alias("total_rating"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .orderBy(F.col("num_countries").desc_nulls_last())
)
