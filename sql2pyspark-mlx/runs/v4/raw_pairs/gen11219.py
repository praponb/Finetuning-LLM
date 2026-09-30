from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("quantity")

result = (
    df_inventory
    .select(
        "*",
        F.row_number().over(w_recent).alias("seq_no"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.lead("quantity", 2).over(w_recent).alias("next_quantity"),
        F.rank().over(w_grp).alias("quantity_rank"),
    )
)
