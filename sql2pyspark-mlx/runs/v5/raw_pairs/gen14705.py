from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.min("price").alias("min_price"),
        F.avg("price").alias("avg_price"),
    )
    .orderBy(F.col("avg_price").desc())
)
