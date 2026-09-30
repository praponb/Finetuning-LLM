from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category", "brand").orderBy("stock_qty", "id")
w4 = Window.partitionBy("supplier_id", "brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("stock_qty").alias("max_product_stock_qty"))

scored = (
    df_products
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.last("stock_qty", ignorenulls=True).over(w2).alias("last_known_stock_qty"),
        F.ntile(10).over(w3).alias("stock_qty_bucket"),
        F.first("stock_qty", ignorenulls=True).over(w4).alias("first_known_stock_qty"),
    )
)

result = (
    scored
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Desk Lamp|Yoga Mat)$"), "GROUP_A").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "GROUP_B").otherwise("PRIORITY").alias("product_name_zone"),
        F.col("max_product_stock_qty"),
    )
    .orderBy("max_product_stock_qty", "price")
)
