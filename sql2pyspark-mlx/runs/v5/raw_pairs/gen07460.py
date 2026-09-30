from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("supplier_id").orderBy("stock_qty_times_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id")
w5 = Window.partitionBy("category", "brand")
w_running = Window.partitionBy("category", "brand").orderBy("cost", "id").rowsBetween(-6, -1)
w_recent = Window.orderBy("supplier_id_max_price")
w_hist = Window.partitionBy("brand", "category").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_products
    .filter(F.month("launch_date").isin([3, 7, 9]) & F.col("brand").isNotNull())
    .select(
        "*",
        (F.col("stock_qty") * F.col("price")).alias("stock_qty_times_price"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"), F.col("c.quantity"))
)

enriched = (
    with_last
    .filter(F.col("brand").isin(["Initech", "Acme"]))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "resolved_brand",
        F.row_number().over(w2).alias("event_seq"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
        F.max("price").over(w4).alias("supplier_id_max_price"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "event_seq",
        F.avg("supplier_id_max_price").over(w5).alias("category_avg_supplier_id_max_price"),
        F.avg("stock_qty").over(w_running).alias("stock_qty_avg_6_prior"),
        F.cume_dist().over(w_recent).alias("supplier_id_max_price_cume_dist"),
        F.round((F.col("supplier_id_max_price") / F.sum("supplier_id_max_price").over(w5)), 4).alias("supplier_id_max_price_share"),
        F.avg("cost").over(w_hist).alias("cost_avg_2"),
    )
)
