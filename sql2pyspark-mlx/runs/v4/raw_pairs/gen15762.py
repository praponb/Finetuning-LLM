from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("row_num", "id")
w3 = Window.partitionBy("store_id").orderBy("dedup_rank", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_last())
w_part = Window.partitionBy("product_id")
w_grp = Window.partitionBy("product_id", "store_id").orderBy("quantity_cume_dist", "id")
w7 = Window.orderBy("quantity_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("store_id").orderBy(F.col("product_id_avg_quantity").desc(), "id")
w9 = Window.partitionBy("store_id", "product_id").orderBy("dedup_rank", "id")

latest = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.avg("quantity").over(w3).alias("quantity_avg_5"),
        F.cume_dist().over(w4).alias("quantity_cume_dist"),
        F.avg("quantity").over(w_part).alias("product_id_avg_quantity"),
    )
)

metrics = (
    enriched
    .withColumn("quantity_cume_dist_bucket", F.ntile(5).over(w_grp))
    .withColumn("running_quantity_avg_5", F.sum("quantity_avg_5").over(w7))
    .withColumn("product_id_avg_quantity_bucket", F.ntile(10).over(w_hist))
    .withColumn("next_quantity_avg_5", F.lead("quantity_avg_5", 2, 0).over(w9))
)

cleaned = (
    metrics
    .filter(F.col("last_restock") >= F.lit("2024-02-17").cast("date"))
    .select(
        "*",
        F.least("dedup_rank", "running_quantity_avg_5").alias("least_dedup_rank_running_quantity_avg_5"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.brand"), F.col("d.price"), F.col("d.category"))
)

base = (
    with_dims
    .filter(F.col("last_restock") >= F.lit("2023-06-29").cast("date"))
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

result = (
    base
    .groupBy(
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
    )
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("product_id_avg_quantity")).otherwise(0)).alias("acme_product_id_avg_quantity"),
        F.sum(F.when(F.col("category") == "Home", F.col("price")).otherwise(0)).alias("home_price"),
    )
    .filter(F.col("acme_product_id_avg_quantity") >= 0)
    .orderBy("quarter_start")
)
