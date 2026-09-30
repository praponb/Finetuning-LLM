from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id").orderBy("quantity_whole", "id")
w_hist = Window.orderBy("last_restock_month_end", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("store_id").orderBy(F.col("quantity_whole").desc(), "id")
w4 = Window.orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2]))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.floor("quantity").alias("quantity_whole"),
    )
)

valid = (
    base
    .filter(F.col("last_restock") < "2023-12-01")
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_whole",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

metrics = (
    valid
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("quantity_avg_2", F.avg("quantity").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_part))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w4))
    .withColumn("quantity_whole_bucket", F.ntile(3).over(w_running))
)

result = (
    metrics
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_whole",
        "row_seq",
        "seq_no",
        "quantity_whole_bucket",
    )
    .orderBy(F.col("quantity_whole_bucket").asc_nulls_first())
)
