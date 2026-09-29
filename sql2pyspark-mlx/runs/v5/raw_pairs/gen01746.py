from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

with_windows = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.count("quantity").over(w).alias("running_count_quantity"),
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.avg("quantity").over(w).alias("running_avg_quantity"),
        F.sum("quantity").over(w).alias("running_sum_quantity"),
    )
)

result = (
    with_windows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "running_count_quantity",
        "running_max_quantity",
        "running_avg_quantity",
        "running_sum_quantity",
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("store_id").desc_nulls_first())
)
