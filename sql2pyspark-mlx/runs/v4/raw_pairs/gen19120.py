from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        F.percent_rank().over(w).alias("last_restock_pct_rank"),
    )
)
