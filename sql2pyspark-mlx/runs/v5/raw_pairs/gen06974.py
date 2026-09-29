from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("brand").orderBy("store_sale_sale_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("price_adjusted", "id").rowsBetween(-3, -1)

base = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("*", F.upper("category").alias("category_upper"))
)

latest_match = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

cleaned = (
    latest_match
    .filter(F.col("stock_qty").between(144, 382))
    .select(
        "*",
        (F.col("cost") - F.col("stock_qty")).alias("cost_minus_stock_qty"),
        F.round((F.col("price") * 100), 2).alias("price_adjusted"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.category",
        "e.brand",
        "e.cost",
        "e.stock_qty",
        "e.launch_date",
        "e.tags",
        "e.supplier_id",
        "e.unit_price",
        "e.cost_minus_stock_qty",
        "e.price_adjusted",
        F.col("p.supplier_name"),
    )
)

with_latest = (
    joined
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.stock_qty",
        "s.launch_date",
        "s.supplier_id",
        "s.price_adjusted",
        "s.supplier_name",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue"),
    )
)

metrics = (
    with_latest
    .filter(F.col("store_sale_sale_date") < "2024-12-07")
    .select(
        "id",
        "stock_qty",
        "supplier_id",
        "price_adjusted",
        "store_sale_sale_date",
        F.count("revenue").over(w3).alias("last4_count_revenue"),
        F.min("revenue").over(w3).alias("last4_min_revenue"),
        F.avg("revenue").over(w3).alias("last4_avg_revenue"),
        F.max("revenue").over(w3).alias("last4_max_revenue"),
        F.avg("revenue").over(w4).alias("revenue_avg_3_prior"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.store_sale_sale_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "supplier_id",
        "last4_count_revenue",
        "last4_min_revenue",
        "last4_avg_revenue",
        "last4_max_revenue",
        "revenue_avg_3_prior",
    )
    .orderBy(F.col("revenue_avg_3_prior").asc_nulls_first())
)
