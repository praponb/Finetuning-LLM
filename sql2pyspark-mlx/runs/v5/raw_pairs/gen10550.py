from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity_whole").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 6, 7]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "product_id", "quantity", "last_restock")
)
