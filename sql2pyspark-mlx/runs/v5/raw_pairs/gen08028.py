from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("quantity").between(65, 103))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.percent_rank().over(w).alias("last_restock_pct_rank"),
    )
    .orderBy("store_id", F.col("id").desc())
)
