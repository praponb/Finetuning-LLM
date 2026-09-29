from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity", "id")
w4 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("seq_no", "id")
w6 = Window.partitionBy("category")
w7 = Window.partitionBy("store_id").orderBy("cumulative_quantity_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id", "city")
w9 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())

enriched = (
    df_inventory
    .withColumn("cumulative_quantity", F.sum("quantity").over(w))
    .withColumn("quantity_avg_2", F.avg("quantity").over(w_running))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("cumulative_quantity_2", F.sum("quantity").over(w4))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

enriched_inventory = (
    ranked
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.product_name"), F.col("p.category"))
)

enriched_inventory_2 = (
    enriched_inventory
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.city"), F.col("b.region"), F.col("b.sqft"))
)

enriched_inventory_3 = (
    enriched_inventory_2
    .select(
        "*",
        F.sum("quantity_avg_2").over(w6).alias("category_sum_quantity_avg_2"),
        F.sum("seq_no").over(w7).alias("cumulative_seq_no"),
        F.avg("cumulative_quantity").over(w8).alias("product_id_avg_cumulative_quantity"),
        F.rank().over(w9).alias("quantity_rank"),
    )
)

with_dims = (
    enriched_inventory_3
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.region").alias("store_region"),
        F.col("d.city").alias("store_city"),
    )
)

with_dims_inventory = (
    with_dims
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.store_name"), F.col("s.city").alias("store_city_2"))
)

result = (
    with_dims_inventory
    .groupBy(
        F.when(F.col("product_id_avg_cumulative_quantity") >= 1159, "PREMIUM").otherwise("STANDARD").alias("product_id_avg_cumulative_quantity_tier"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("quantity_rank")).otherwise(0)).alias("sports_quantity_rank"),
        F.percentile_approx("category_sum_quantity_avg_2", [0.25, 0.5, 0.75]).alias("category_sum_quantity_avg_2_quartiles"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("category_sum_quantity_avg_2"), 2).alias("avg_category_sum_quantity_avg_2"),
        F.count_if(F.col("store_city") == "Singapore").alias("singapore_events"),
        F.sum("cumulative_quantity").alias("total_cumulative_quantity"),
    )
)
