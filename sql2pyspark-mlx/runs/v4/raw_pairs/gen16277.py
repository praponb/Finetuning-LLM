from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("units").alias("sum_store_sale_units"))

with_windows = (
    df_inventory
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w2))
)

result = (
    with_windows
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "cumulative_quantity",
        F.col("sum_store_sale_units"),
    )
    .orderBy(F.col("cumulative_quantity").asc_nulls_first())
)
