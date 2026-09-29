from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .select(
        "id",
        "last_restock",
        F.count("quantity").over(w_part).alias("running_count_quantity"),
        F.max("quantity").over(w_part).alias("running_max_quantity"),
        F.avg("quantity").over(w_part).alias("running_avg_quantity"),
    )
)
