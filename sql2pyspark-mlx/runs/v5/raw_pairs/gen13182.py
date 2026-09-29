from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

scored = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.sum("revenue").over(w).alias("cumulative_revenue"),
        F.corr("units", "revenue").over(w_running).alias("corr_units_revenue"),
    )
)

result = (
    scored
    .join(quantity_by_product_id, on="product_id", how="left")
    .select("id", "product_id", "sale_date", "units", F.col("avg_inventory_quantity"))
)
