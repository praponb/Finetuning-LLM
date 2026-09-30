from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity_filled", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("store_id")
w_grp = Window.partitionBy("product_id", "store_id")

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-06-18").cast("date")) & F.col("store_id").isin([1, 4]))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.ntile(4).over(w).alias("quantity_bucket"),
        F.avg("quantity").over(w2).alias("quantity_avg_2"),
    )
)

kept_keys = (
    enriched
    .select("store_id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 3, 5, 6]))
        .select("store_id", "product_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("store_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w3))
    .withColumn("quantity_bucket_share", F.round((F.col("quantity_bucket") / F.sum("quantity_bucket").over(w3)), 4))
    .withColumn("product_id_sum_quantity_avg_2", F.sum("quantity_avg_2").over(w_grp))
)

base = (
    scored
    .filter(F.col("last_restock") >= F.lit("2024-03-21").cast("date"))
    .select(
        "*",
        (F.col("quantity") * F.col("quantity_avg_2")).alias("quantity_times_quantity_avg_2"),
    )
)

remaining_keys = (
    base
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("last_restock") < "2025-03-01")
        .select("store_id", "id")
    )
)

reconciled_inventory = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    reconciled_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.stock_qty"))
)

report = (
    joined
    .select(
        "store_id",
        "quantity",
        "last_restock",
        "quantity_avg_2",
        "store_id_stddev_samp_quantity",
        "product_id_sum_quantity_avg_2",
    )
)

result = (
    report
    .select(
        "last_restock",
        "store_id_stddev_samp_quantity",
        "product_id_sum_quantity_avg_2",
    )
)
