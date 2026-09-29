from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("quantity").asc_nulls_first())
w_grp = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w_hist = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc())
w5 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "*",
        F.rank().over(w_part).alias("quantity_rank"),
        F.avg("quantity").over(w_grp).alias("quantity_avg_6"),
        F.row_number().over(w3).alias("row_seq"),
        F.rank().over(w_hist).alias("quantity_rank_2"),
        F.first("quantity", ignorenulls=True).over(w5).alias("first_known_quantity"),
    )
)
