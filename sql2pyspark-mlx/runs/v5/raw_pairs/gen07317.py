from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("cost_plus_stock_qty_per_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("cost").alias("sum_product_cost"))

base = (
    df_products
    .filter(F.col("brand").isNotNull() & ~F.col("brand").isin(["Stark", "Initech"]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.round((F.col("stock_qty") / F.col("price")), 2).alias("stock_qty_per_price"),
        F.when(F.col("price") >= 980, "High").when(F.col("price") >= 260, "Medium").otherwise("Low").alias("price_band"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.city"))
)

filtered = (
    joined
    .filter(F.array_contains("tags", "sale"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "stock_qty_per_price",
        "price_band",
        F.array_contains("tags", "premium").alias("is_premium"),
        (F.col("cost") + F.col("stock_qty_per_price")).alias("cost_plus_stock_qty_per_price"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.col("stock_qty").between(189, 340))
    .select(
        "*",
        F.sum("cost_plus_stock_qty_per_price").over(w).alias("running_sum_cost_plus_stock_qty_per_price"),
        F.avg("cost_plus_stock_qty_per_price").over(w).alias("running_avg_cost_plus_stock_qty_per_price"),
        F.min("cost_plus_stock_qty_per_price").over(w).alias("running_min_cost_plus_stock_qty_per_price"),
    )
)

result = (
    with_windows
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "stock_qty_per_price",
        "price_band",
        "is_premium",
        F.col("sum_product_cost"),
    )
)
