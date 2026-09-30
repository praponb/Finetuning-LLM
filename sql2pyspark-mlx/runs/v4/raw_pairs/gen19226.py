from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity_filled").desc())
w_grp = Window.partitionBy("store_id").orderBy("quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity_filled").desc(), "id")
w_ordered = Window.partitionBy("product_id").orderBy("quantity_filled", "id").rowsBetween(-3, -1)
w_hist = Window.orderBy("quantity_filled")
w6 = Window.partitionBy("product_id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

valid = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.sum("quantity_filled").over(w_grp).alias("cumulative_quantity_filled"),
        F.ntile(10).over(w3).alias("quantity_filled_bucket"),
        F.avg("quantity_filled").over(w_ordered).alias("quantity_filled_avg_3_prior"),
        F.percent_rank().over(w_hist).alias("quantity_filled_percent_rank"),
        F.min("quantity_filled").over(w6).alias("product_id_min_quantity_filled"),
    )
)

result = (
    with_windows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "cumulative_quantity_filled",
        "quantity_filled_avg_3_prior",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("product_id").desc())
)
