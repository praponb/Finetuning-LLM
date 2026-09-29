from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock_period", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())

valid = (
    df_inventory
    .filter(F.col("last_restock") < "2023-06-01")
    .select(
        "*",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
)

result = (
    valid
    .withColumn("quantity_avg_2_prior", F.avg("quantity").over(w))
    .withColumn("store_id_count_quantity", F.count("quantity").over(w2))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w3))
)
