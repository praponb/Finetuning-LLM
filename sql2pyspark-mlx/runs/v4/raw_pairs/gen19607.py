from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("days_since_last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([4, 6]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w).alias("days_since_last_restock_bucket"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
