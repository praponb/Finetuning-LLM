from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("last_restock", "id")

enriched = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.avg("quantity").over(w).alias("last3_avg_quantity"),
        F.max("quantity").over(w).alias("last3_max_quantity"),
        F.sum("quantity").over(w).alias("last3_sum_quantity"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    enriched
    .select("id", "product_id", "quantity", "last3_max_quantity", "event_seq")
    .orderBy(F.col("last3_max_quantity").desc())
)
