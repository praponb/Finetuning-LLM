from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w_running = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_store_id = df_orders.groupBy("store_id").agg(F.avg("discount").alias("avg_order_discount"))

scored = (
    df_inventory
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("quantity", ignorenulls=True).over(w_running).alias("last_known_quantity"),
    )
)

result = (
    scored
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "row_seq",
        "last_known_quantity",
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("quantity").desc_nulls_last(), "row_seq")
)
