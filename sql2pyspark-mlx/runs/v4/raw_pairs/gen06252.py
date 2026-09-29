from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id")
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("units").alias("sum_store_sale_units"))

with_windows = (
    df_inventory
    .select(
        "*",
        F.ntile(10).over(w).alias("quantity_bucket"),
        F.max("quantity").over(w2).alias("store_id_max_quantity"),
    )
)

result = (
    with_windows
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_bucket",
        F.col("sum_store_sale_units"),
    )
)
