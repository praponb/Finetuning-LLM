from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("sale_date", "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.min("quantity").alias("min_inventory_quantity"))

enriched = (
    df_store_sales
    .filter(F.col("sale_date") <= "2024-08-01")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.sum("units").over(w).alias("running_units"),
        F.lag("revenue", 1).over(w2).alias("prev_revenue"),
    )
)

result = (
    enriched
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "units",
        "running_units",
        "prev_revenue",
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("min_inventory_quantity").asc_nulls_last())
)
