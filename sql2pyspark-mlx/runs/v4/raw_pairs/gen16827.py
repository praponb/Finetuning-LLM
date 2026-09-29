from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy(F.col("last_restock_weekofyear").desc())
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id_min_quantity_tier").orderBy(F.col("row_count").desc())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

cleaned = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 6]))
    .select("*", F.weekofyear("last_restock").alias("last_restock_weekofyear"))
)

kept_keys = (
    cleaned
    .select("store_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.col("quantity") <= 106)
        .select("store_id", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("product_id_avg_last_restock_weekofyear", F.avg("last_restock_weekofyear").over(w))
    .withColumn("last_restock_weekofyear_dense_rank", F.dense_rank().over(w2))
    .withColumn("store_id_min_quantity", F.min("quantity").over(w3))
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("store_id_min_quantity") >= 250, "LARGE").otherwise("SMALL").alias("store_id_min_quantity_tier"),
        "store_id",
    )
    .agg(
        F.percentile_approx("store_id_min_quantity", [0.25, 0.5, 0.75]).alias("store_id_min_quantity_quartiles"),
        F.count("*").alias("row_count"),
        F.round(F.avg("last_restock_weekofyear_dense_rank"), 2).alias("avg_last_restock_weekofyear_dense_rank"),
        F.percentile_approx("product_id_avg_last_restock_weekofyear", 0.9).alias("product_id_avg_last_restock_weekofyear_p90"),
        F.avg("product_id_avg_last_restock_weekofyear").alias("avg_product_id_avg_last_restock_weekofyear"),
    )
    .filter(F.col("product_id_avg_last_restock_weekofyear_p90") < 0)
    .withColumn("row_count_rank", F.rank().over(w4))
)

result = (
    summary
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "store_id_min_quantity_tier",
        "store_id",
        "row_count",
        "avg_last_restock_weekofyear_dense_rank",
        "product_id_avg_last_restock_weekofyear_p90",
        "avg_product_id_avg_last_restock_weekofyear",
        "row_count_rank",
        F.col("sum_inventory_quantity"),
    )
)
