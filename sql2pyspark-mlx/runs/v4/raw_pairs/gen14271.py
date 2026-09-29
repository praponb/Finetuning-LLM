from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("last_restock_year", "id")
w3 = Window.partitionBy("store_id").orderBy("quantity_filled", "id")
w4 = Window.partitionBy("region", "store_id")
w5 = Window.partitionBy("store_id", "region").orderBy("last_restock_year", "id")
w6 = Window.partitionBy("region").orderBy("last_restock")
w7 = Window.partitionBy("region").orderBy(F.col("quantity").desc())
w8 = Window.orderBy(F.col("prev_quantity_filled").asc_nulls_first())
w9 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w10 = Window.partitionBy("store_id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-09-01")))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.year("last_restock").alias("last_restock_year"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.sqft"), F.col("p.region"))
)

scored = (
    with_dims
    .withColumn("last_known_last_restock_year", F.last("last_restock_year", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("quantity_filled_bucket", F.ntile(10).over(w3))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4))
    .withColumn("prev_quantity_filled", F.lag("quantity_filled", 1).over(w5))
)

latest = (
    scored
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    latest
    .select("store_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock").between("2024-06-01", "2025-04-07"))
        .select("store_id", "id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter((F.col("last_restock") > F.lit("2023-06-15").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "last_restock_year",
        "sqft",
        "region",
        "last_known_last_restock_year",
        "row_seq",
        "quantity_filled_bucket",
        "sqft_share",
        "prev_quantity_filled",
        "dedup_rank",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled_2"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

with_windows = (
    filtered
    .withColumn("quantity_cume_dist", F.cume_dist().over(w7))
    .withColumn("prev_quantity_filled_cume_dist", F.cume_dist().over(w8))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w9))
    .withColumn("store_id_min_last_known_last_restock_year", F.min("last_known_last_restock_year").over(w10))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("quantity_filled_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("quantity_filled_bucket_tier"),
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.round(F.avg("quantity_filled_bucket"), 2).alias("avg_quantity_filled_bucket"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .orderBy(F.col("quantity_filled_bucket_tier").desc_nulls_last())
)
