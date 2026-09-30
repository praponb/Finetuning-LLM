from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("stock_qty", "id")
w2 = Window.orderBy("last_restock", "id")

with_dims = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.region"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.last_restock",
        "s.region",
        F.col("b.stock_qty"),
        F.col("b.launch_date"),
        F.col("b.category"),
    )
)

metrics = (
    joined
    .filter(F.col("last_restock").between("2023-06-01", "2024-10-01"))
    .select(
        "*",
        F.lead("stock_qty", 1).over(w_ordered).alias("next_stock_qty"),
        F.lead("stock_qty", 2).over(w2).alias("next_stock_qty_2"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.store_name"))
)

result = (
    enriched
    .groupBy(F.date_trunc("YEAR", "last_restock").alias("year_start"))
    .agg(
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.max("next_stock_qty").alias("max_next_stock_qty"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
    )
    .orderBy(F.col("electronics_rows").desc_nulls_first())
)
