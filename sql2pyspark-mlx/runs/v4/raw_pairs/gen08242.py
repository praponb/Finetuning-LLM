from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.orderBy("price")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc())
w4 = Window.orderBy(F.col("price").desc())
w5 = Window.partitionBy("brand")

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([9, 13, 33, 36])), "left_anti")
)

remaining_keys = (
    eligible
    .select("id", "category")
    .intersect(
        df_products
        .filter(F.col("cost") > 260)
        .select("id", "category")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.cume_dist().over(w2).alias("price_cume_dist"),
        F.rank().over(w3).alias("stock_qty_rank"),
    )
)

eligible_products = (
    enriched
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isNull()), "left_anti")
)

metrics = (
    eligible_products
    .select(
        "*",
        F.rank().over(w4).alias("price_rank"),
        F.round((F.col("stock_qty_rank") / F.sum("stock_qty_rank").over(w)), 4).alias("stock_qty_rank_share"),
        F.corr("stock_qty", "price").over(w5).alias("corr_stock_qty_price"),
    )
)

result = (
    metrics
    .groupBy("category")
    .pivot("brand", ["Globex", "Stark"])
    .agg(F.sum("corr_stock_qty_price"))
)
