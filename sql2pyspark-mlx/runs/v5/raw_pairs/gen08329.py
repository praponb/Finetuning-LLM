from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-03-18"))) & (F.col("last_restock") >= "2025-07-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)

result = (
    valid
    .filter(F.col("last_restock") >= "2024-02-01")
    .select(
        "id",
        "store_id",
        "last_restock",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("quantity").over(w3).alias("running_quantity"),
        F.ntile(10).over(w4).alias("quantity_bucket"),
    )
)
