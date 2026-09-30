from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("quantity").between(79, 81) & (F.col("quantity") != 114))
    .withColumn("quantity_whole", F.floor("quantity"))
)

product_id_summary = (
    recent_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity_whole"), 2).alias("stddev_quantity_whole"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = product_id_summary.orderBy(F.col("max_quantity").desc())
