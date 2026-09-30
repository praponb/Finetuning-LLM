from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("attributes").alias("key", "value"))

result = (
    lines
    .groupBy(F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.count_if(F.col("key") == "utm_source").alias("utm_source_events"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.min("created_at").alias("first_created_at"),
        F.count_if(F.col("key") == "weight_kg").alias("weight_kg_events"),
    )
    .filter(F.col("weight_kg_rows") > 1)
)
