from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock").between("2024-09-18", "2025-01-01"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 194, "High").when(F.col("quantity") >= 119, "Medium").otherwise("Low"))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.category"), F.col("p.launch_date"), F.col("p.brand"))
)

joined = (
    enriched
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .filter(F.col("d.price") > 890)
    .select("a.*", F.col("d.category").alias("product_category"))
)

stats = (
    joined
    .groupBy(
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.stddev_samp("quantity").alias("quantity_stddev_2"),
    )
    .filter(F.col("quantity_stddev_2") < 1000000)
)

result = (
    stats
    .select("quarter_start", "activity_month", "num_categories", "quantity_stddev_2")
)
