from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("last4_max_quantity").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("last4_max_quantity").desc_nulls_first())
w_ordered = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w_running = Window.partitionBy("product_id")
w6 = Window.partitionBy("region").orderBy(F.col("last4_sum_quantity").desc(), "id")

enriched = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.region"), F.col("p.store_name"))
)

scored = (
    enriched
    .filter(F.col("store_id") == 2)
    .select(
        "id",
        "product_id",
        "last_restock",
        "region",
        F.sum("quantity").over(w).alias("last4_sum_quantity"),
        F.max("quantity").over(w).alias("last4_max_quantity"),
        F.min("quantity").over(w).alias("last4_min_quantity"),
    )
)

enriched_inventory = (
    scored
    .select(
        "*",
        F.ntile(4).over(w2).alias("last4_max_quantity_bucket"),
        F.rank().over(w3).alias("last4_max_quantity_rank"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.sum("last4_sum_quantity").over(w_running).alias("product_id_sum_last4_sum_quantity"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

joined = (
    enriched_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.last_restock",
        "s.last4_sum_quantity",
        "s.last4_max_quantity",
        "s.last4_min_quantity",
        "s.last4_max_quantity_rank",
        F.col("p.cost"),
        F.col("p.stock_qty"),
    )
)

valid = (
    joined
    .filter(F.col("last4_max_quantity") < 180)
    .select(
        "id",
        "last4_max_quantity",
        "last4_max_quantity_rank",
        (F.col("last4_max_quantity") * F.col("stock_qty")).alias("last4_max_quantity_times_stock_qty"),
    )
)

result = (
    valid
    .rollup(
        F.when(F.col("last4_max_quantity") >= 204, "PREMIUM").otherwise("STANDARD").alias("last4_max_quantity_tier"),
        F.when(F.col("last4_max_quantity_rank") >= 7, "HIGH").otherwise("LOW").alias("last4_max_quantity_rank_tier"),
    )
    .agg(
        F.avg("last4_max_quantity_rank").alias("avg_last4_max_quantity_rank"),
        F.percentile_approx("last4_max_quantity", [0.25, 0.5, 0.75]).alias("last4_max_quantity_quartiles"),
    )
    .orderBy(F.col("last4_max_quantity_tier").desc())
)
