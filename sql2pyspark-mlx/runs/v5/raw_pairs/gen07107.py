from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("stock_qty").desc(), "id")
w_ordered = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("brand")
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

scored = (
    df_products
    .select(
        "*",
        F.ntile(4).over(w).alias("stock_qty_bucket"),
        F.last("price", ignorenulls=True).over(w_ordered).alias("last_known_price"),
        F.corr("price", "cost").over(w_recent).alias("corr_price_cost"),
    )
)

result = (
    scored
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "stock_qty_bucket",
        "last_known_price",
        "corr_price_cost",
        F.when(F.col("category").rlike("^(Electronics|Sports)$"), "CORE").when(F.col("category").rlike("^(Home|Books)$"), "PRIORITY").otherwise("GROUP_B").alias("category_zone"),
        F.col("max_product_price"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
