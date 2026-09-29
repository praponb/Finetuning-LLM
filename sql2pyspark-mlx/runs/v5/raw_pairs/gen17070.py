from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("store_id", "product_id")

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
        F.max("quantity").over(w3).alias("store_id_max_quantity"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("week", "last_restock").alias("week_start"))
    .agg(
        F.round(F.avg("product_id_sum_quantity"), 2).alias("avg_product_id_sum_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.round(F.avg("store_id_max_quantity"), 2).alias("avg_store_id_max_quantity"),
        F.percentile_approx("store_id_max_quantity", [0.25, 0.5, 0.75]).alias("store_id_max_quantity_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("avg_product_id_sum_quantity") > 0)
)
