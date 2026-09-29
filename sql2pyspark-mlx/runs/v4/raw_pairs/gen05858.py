from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("city", "supplier_id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

scored = (
    df_products
    .select(
        "*",
        F.ntile(4).over(w_recent).alias("stock_qty_bucket"),
        F.first("price", ignorenulls=True).over(w2).alias("first_known_price"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.city"))
)

matched_keys = (
    enriched
    .select("category", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("brand").isin(["Acme", "Initech", "Stark"]))
        .select("category", "supplier_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .withColumn("brand_avg_stock_qty_bucket", F.avg("stock_qty_bucket").over(w3))
    .withColumn("city_avg_price", F.avg("price").over(w4))
    .withColumn("city_count_stock_qty_bucket", F.count("stock_qty_bucket").over(w5))
)

result = (
    with_windows
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "stock_qty_bucket",
        "first_known_price",
        "brand_avg_stock_qty_bucket",
        F.col("avg_product_cost"),
    )
    .orderBy(F.col("avg_product_cost").desc())
)
