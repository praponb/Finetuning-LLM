from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.orderBy(F.col("quantity").desc_nulls_last())

filtered = (
    df_inventory
    .filter(F.col("store_id") == 5)
    .select(
        "id",
        "quantity",
        "last_restock",
        F.date_trunc("YEAR", "last_restock").alias("last_restock_period"),
    )
)

result = (
    filtered
    .select(
        "id",
        "last_restock",
        F.ntile(5).over(w).alias("quantity_bucket"),
        F.avg("quantity").over(w2).alias("quantity_avg_6"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.max("quantity").over(w4).alias("quantity_max_30d"),
        F.cume_dist().over(w5).alias("quantity_cume_dist"),
    )
)
