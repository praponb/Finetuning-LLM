from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-07-15", "2025-05-01"))
    .select("price", F.explode("tags").alias("tag"))
    .groupBy("tag")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .orderBy(F.col("avg_price").desc(), "tag")
)
