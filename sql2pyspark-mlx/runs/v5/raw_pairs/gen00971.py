from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("quantity") > 186)
    .select(
        "id",
        "store_id",
        "product_id",
        F.min("quantity").over(w_grp).alias("last3_min_quantity"),
        F.max("quantity").over(w_grp).alias("last3_max_quantity"),
        F.sum("quantity").over(w_grp).alias("last3_sum_quantity"),
        F.count("quantity").over(w_grp).alias("last3_count_quantity"),
    )
)
