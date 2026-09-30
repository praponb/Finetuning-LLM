from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").asc_nulls_last())
w2 = Window.partitionBy("order_id")
w_hist = Window.partitionBy("order_id").orderBy("unit_price", "id")
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("order_id_count_unit_price").desc(), "id")
w6 = Window.orderBy("quantity_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy("quantity", "id")
w8 = Window.partitionBy("product_category")
w9 = Window.partitionBy("channel")
w10 = Window.partitionBy("product_id")

enriched = (
    df_order_items
    .filter(F.col("unit_price").between(240, 350))
    .select(
        "*",
        F.rank().over(w).alias("quantity_rank"),
        F.count("unit_price").over(w2).alias("order_id_count_unit_price"),
        F.lag("unit_price", 1).over(w_hist).alias("prev_unit_price"),
        F.last("quantity", ignorenulls=True).over(w4).alias("last_known_quantity"),
    )
)

enriched_order_items = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_rank",
        "order_id_count_unit_price",
        "last_known_quantity",
        F.ntile(5).over(w5).alias("order_id_count_unit_price_bucket"),
        F.sum("unit_price").over(w6).alias("cumulative_unit_price"),
        F.ntile(10).over(w_running).alias("quantity_bucket"),
    )
)

remaining_keys = (
    enriched_order_items
    .select("id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("product_id").isin([17, 33]))
        .select("id", "product_id")
    )
)

reconciled = (
    enriched_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched_order_items_2 = (
    reconciled
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select(
        "src.order_id",
        "src.product_id",
        "src.quantity",
        "src.order_id_count_unit_price",
        "src.last_known_quantity",
        "src.order_id_count_unit_price_bucket",
        "src.cumulative_unit_price",
        "src.quantity_bucket",
        F.col("p.cost"),
        F.col("p.category"),
        F.col("p.product_name"),
    )
)

joined = (
    enriched_order_items_2
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select(
        "a.order_id",
        "a.product_id",
        "a.quantity",
        "a.category",
        "a.product_name",
        F.col("p.stock_qty"),
    )
)

enriched_order_items_3 = (
    joined
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.channel"))
)

with_dims = (
    enriched_order_items_3
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.category").alias("product_category"))
)

result = (
    with_dims
    .select(
        "order_id",
        "quantity",
        "product_name",
        "stock_qty",
        "product_category",
        F.avg("stock_qty").over(w8).alias("product_category_avg_stock_qty"),
        F.round((F.col("quantity") / F.sum("quantity").over(w9)), 4).alias("quantity_share"),
        F.round((F.col("quantity") / F.sum("quantity").over(w10)), 4).alias("quantity_share_2"),
    )
)
