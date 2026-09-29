from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 6]))
    .select(
        "id",
        "store_id",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
        F.rank().over(w4).alias("quantity_rank"),
    )
)
