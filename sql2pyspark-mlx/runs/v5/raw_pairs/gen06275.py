from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        F.rank().over(w).alias("quantity_rank"),
        F.percent_rank().over(w2).alias("quantity_percent_rank"),
    )
)
