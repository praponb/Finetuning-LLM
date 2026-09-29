from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").asc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first())
w4 = Window.partitionBy("product_id").orderBy(F.col("first_known_quantity").desc())
w5 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("first_known_quantity").desc(), "id")
w7 = Window.partitionBy("product_id").orderBy("quantity_cume_dist", "id")
w8 = Window.partitionBy("product_id").orderBy(F.col("first_known_quantity").desc_nulls_last())

scored = (
    df_inventory
    .withColumn("quantity_cume_dist", F.cume_dist().over(w))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w2))
    .withColumn("quantity_cume_dist_2", F.cume_dist().over(w3))
)

enriched = (
    scored
    .select(
        "*",
        F.percent_rank().over(w4).alias("first_known_quantity_percent_rank"),
        F.lag("quantity", 2).over(w5).alias("prev_quantity"),
        F.ntile(4).over(w6).alias("first_known_quantity_bucket"),
        F.ntile(4).over(w7).alias("quantity_cume_dist_bucket"),
        F.dense_rank().over(w8).alias("first_known_quantity_dense_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("last_restock") > F.lit("2024-01-19").cast("date"))
    .withColumn("adjusted_first_known_quantity_dense_rank", F.round((F.col("first_known_quantity_dense_rank") * 1.1), 2))
)

enriched_inventory = (
    valid
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.price"), F.col("d.brand"))
)

totals = (
    enriched_inventory
    .groupBy(
        F.when(F.col("first_known_quantity") >= 263, "HIGH").otherwise("LOW").alias("first_known_quantity_tier"),
        F.when(F.col("adjusted_first_known_quantity_dense_rank") >= 82, "PREMIUM").otherwise("STANDARD").alias("adjusted_first_known_quantity_dense_rank_tier"),
        F.when(F.col("quantity_cume_dist_2") >= 0.6, "LARGE").otherwise("SMALL").alias("quantity_cume_dist_2_tier"),
    )
    .agg(
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.avg(F.when(F.col("brand") == "Initech", F.col("first_known_quantity_dense_rank"))).alias("initech_avg_first_known_quantity_dense_rank"),
    )
)

result = (
    totals
    .select(
        "first_known_quantity_tier",
        "adjusted_first_known_quantity_dense_rank_tier",
        "quantity_cume_dist_2_tier",
        "quantity_p95",
        "initech_avg_first_known_quantity_dense_rank",
    )
)
