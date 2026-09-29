from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("quantity")
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.min("revenue").alias("min_store_sale_revenue"))

with_windows = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4]))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w_part))
    .withColumn("running_quantity", F.sum("quantity").over(w3))
    .withColumn("quantity_rank", F.rank().over(w4))
)

result = (
    with_windows
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity",
        "last_known_quantity",
        "running_quantity",
        "quantity_rank",
        F.col("min_store_sale_revenue"),
    )
)
