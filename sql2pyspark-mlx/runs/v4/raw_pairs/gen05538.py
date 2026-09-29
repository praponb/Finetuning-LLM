from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_num"),
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    latest
    .withColumn("running_quantity", F.sum("quantity").over(w2))
    .withColumn("quantity_bucket", F.ntile(4).over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "running_quantity",
        "quantity_bucket",
    )
    .orderBy(F.col("running_quantity").desc())
)
