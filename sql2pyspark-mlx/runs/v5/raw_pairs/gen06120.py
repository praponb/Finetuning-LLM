from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w_recent = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_inventory
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .withColumn("quantity_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("running_quantity", F.sum("quantity").over(w3))
)

remaining_keys = (
    metrics
    .select("product_id", "id")
    .intersect(
        df_inventory
        .filter(F.col("product_id").isin([5, 8]))
        .select("product_id", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.opened_date"), F.col("p.sqft"))
)

result = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .cube(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.sqft")), 2).alias("avg_sqft"),
        F.round(F.sum(F.col("a.quantity")), 2).alias("total_quantity"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("avg_sqft").desc_nulls_first())
)
