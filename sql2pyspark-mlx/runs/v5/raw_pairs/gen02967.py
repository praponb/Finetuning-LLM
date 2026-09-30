from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity_whole").desc())
w_part = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity_whole").desc_nulls_last())
w_grp = Window.partitionBy("product_id")
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity_whole").asc_nulls_last())

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-08-10")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"),
        F.when(F.col("quantity") >= 253, "High").when(F.col("quantity") >= 70, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") <= "2023-06-01")
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_inventory = (
    kept_rows
    .filter(F.col("last_restock") <= "2025-10-01")
    .select(
        "*",
        F.when(F.col("adjusted_quantity") >= 87, "High").when(F.col("adjusted_quantity") >= 15, "Medium").otherwise("Low").alias("adjusted_quantity_tier"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

metrics = (
    valid_inventory
    .filter(F.col("quantity").between(142, 233))
    .withColumn("quantity_whole_rank", F.rank().over(w))
    .withColumn("quantity_whole_cume_dist", F.cume_dist().over(w_part))
    .withColumn("product_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w_grp))
)

cleaned = (
    metrics
    .filter(F.col("last_restock") >= F.lit("2023-06-28").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "adjusted_quantity",
        "quantity_level",
        "adjusted_quantity_tier",
        "quantity_whole",
        "quantity_whole_rank",
        "quantity_whole_cume_dist",
        F.col("quantity").cast("double").alias("quantity_num"),
        (F.col("quantity_whole_cume_dist") * F.col("adjusted_quantity")).alias("quantity_whole_cume_dist_times_adjusted_quantity"),
    )
)

with_windows = cleaned.withColumn("quantity_whole_cume_dist_2", F.cume_dist().over(w4))

with_dims = (
    with_windows
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.quantity_level",
        "src.adjusted_quantity_tier",
        "src.quantity_whole",
        "src.quantity_whole_cume_dist_times_adjusted_quantity",
        "src.quantity_whole_cume_dist_2",
        F.col("p.sqft"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("quantity_whole") >= 90, "LARGE").otherwise("SMALL").alias("quantity_whole_tier"),
        "store_id",
        F.when(F.col("quantity_whole_cume_dist_times_adjusted_quantity") >= 71, "LARGE").otherwise("SMALL").alias("quantity_whole_cume_dist_times_adjusted_quantity_tier"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
