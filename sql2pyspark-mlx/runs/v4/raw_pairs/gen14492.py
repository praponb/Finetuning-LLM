from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

with_windows = (
    df_store_sales
    .select(
        "*",
        F.ntile(3).over(w).alias("units_bucket"),
        F.first("units", ignorenulls=True).over(w2).alias("first_known_units"),
        F.ntile(5).over(w3).alias("units_bucket_2"),
    )
)

result = (
    with_windows
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "units_bucket",
        "first_known_units",
        F.col("sum_order_item_unit_price"),
    )
)
