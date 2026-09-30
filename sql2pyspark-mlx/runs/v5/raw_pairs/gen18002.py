from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())

metrics = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.avg("quantity").over(w).alias("quantity_avg_2_prior"),
        F.percent_rank().over(w2).alias("quantity_percent_rank"),
    )
)

result = metrics.select("id", "store_id", "quantity", "last_restock")
