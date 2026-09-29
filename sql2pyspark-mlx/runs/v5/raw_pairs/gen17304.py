from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("quantity_whole", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("quantity") <= 148)
    .select(
        "id",
        "store_id",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"),
    )
)

result = (
    filtered
    .select(
        "id",
        "quantity_whole",
        "quantity_adjusted",
        F.count("quantity_adjusted").over(w_part).alias("last3_count_quantity_adjusted"),
        F.avg("quantity_adjusted").over(w_part).alias("last3_avg_quantity_adjusted"),
        F.max("quantity_adjusted").over(w_part).alias("last3_max_quantity_adjusted"),
        F.min("quantity_adjusted").over(w_part).alias("last3_min_quantity_adjusted"),
        F.last("quantity_adjusted", ignorenulls=True).over(w2).alias("last_known_quantity_adjusted"),
    )
)
