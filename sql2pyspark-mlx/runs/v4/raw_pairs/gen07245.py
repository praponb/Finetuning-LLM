from pyspark.sql import functions as F

src = df_inventory.filter(F.col("quantity").isNotNull() | F.col("store_id").isin([3, 6]))

agg = (
    src
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = agg.select("store_id", "quantity_count")
