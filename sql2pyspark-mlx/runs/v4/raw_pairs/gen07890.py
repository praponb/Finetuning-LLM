from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("prev_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy(F.col("last_restock_month_end").desc(), "id")

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .select("*", F.last_day("last_restock").alias("last_restock_month_end"))
)

metrics = (
    base
    .withColumn("quantity_dense_rank", F.dense_rank().over(w))
    .withColumn("prev_quantity", F.lag("quantity", 2).over(w2))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w3))
)

metrics_inventory = (
    metrics
    .withColumn("cumulative_prev_quantity", F.sum("prev_quantity").over(w4))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w5))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

enriched = (
    metrics_inventory
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.city"), F.col("p.store_name"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.region"), F.col("s.city").alias("store_city"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("prev_quantity") >= 248, "HIGH").otherwise("LOW").alias("prev_quantity_tier"),
    )
    .agg(
        F.min("seq_no").alias("min_seq_no"),
        F.sum(F.when(F.col("region") == "East", F.col("last_known_quantity"))).alias("east_total_last_known_quantity"),
        F.count_if(F.col("region") == "West").alias("west_events"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.count(F.when(F.col("store_city") == "Bangkok", 1)).alias("bangkok_count_2"),
    )
)
