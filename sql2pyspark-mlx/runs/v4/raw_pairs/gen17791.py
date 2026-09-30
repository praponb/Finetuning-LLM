from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "HIGH").otherwise("LOW").alias("rating_tier"),
        "country",
    )
    .agg(
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.count("*").alias("event_count"),
        F.max(F.when(F.col("city") == "New York", F.col("rating"))).alias("new_york_max_rating"),
    )
)
