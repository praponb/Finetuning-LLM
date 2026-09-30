from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id", "product_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("product_id", "store_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("last_restock") > "2023-10-01")
    .select(
        "id",
        "store_id",
        "product_id",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

result = (
    base
    .select(
        "id",
        "product_id",
        "quantity_rounded",
        F.avg("quantity_rounded").over(w_grp).alias("running_avg_quantity_rounded"),
        F.sum("quantity_rounded").over(w_grp).alias("running_sum_quantity_rounded"),
        F.min("quantity_rounded").over(w_grp).alias("running_min_quantity_rounded"),
        F.last("quantity_rounded", ignorenulls=True).over(w_grp_2).alias("last_known_quantity_rounded"),
    )
)
