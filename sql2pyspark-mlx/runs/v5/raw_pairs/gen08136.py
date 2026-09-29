from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id")
w_grp = Window.orderBy(F.col("running_min_quantity").desc_nulls_first())
w_hist = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-09-13")))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.brand"), F.col("d.launch_date"))
)

scored = (
    with_dims
    .select(
        "*",
        F.avg("quantity").over(w).alias("running_avg_quantity"),
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.min("quantity").over(w).alias("running_min_quantity"),
    )
)

scored_inventory = (
    scored
    .select(
        "id",
        "store_id",
        "last_restock",
        "brand",
        "launch_date",
        "running_avg_quantity",
        "running_min_quantity",
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.sum("running_avg_quantity").over(w3).alias("product_id_sum_running_avg_quantity"),
    )
)

result = (
    scored_inventory
    .select(
        "id",
        "last_restock",
        "brand",
        "running_avg_quantity",
        "running_min_quantity",
        F.cume_dist().over(w_grp).alias("running_min_quantity_cume_dist"),
        F.sum("running_min_quantity").over(w_hist).alias("running_running_min_quantity"),
    )
)
