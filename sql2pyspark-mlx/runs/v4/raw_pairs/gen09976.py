from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_whole", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w5 = Window.orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id", "store_id").orderBy("quantity_whole", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("product_id").orderBy("quantity_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id", "store_id").orderBy(F.col("first_known_quantity_adjusted").desc(), "id")
w9 = Window.partitionBy("store_id")
w10 = Window.partitionBy("product_id").orderBy("cumulative_quantity_whole", "id")
w11 = Window.partitionBy("store_id").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("quantity").between(117, 123))
    .select(
        "*",
        F.round((F.col("quantity") * 1.07), 2).alias("quantity_adjusted"),
        F.ceil("quantity").alias("quantity_whole"),
    )
)

with_windows = (
    valid
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("event_seq_2", F.row_number().over(w2))
)

enriched = (
    with_windows
    .withColumn("cumulative_quantity_whole", F.sum("quantity_whole").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("first_known_quantity_adjusted", F.first("quantity_adjusted", ignorenulls=True).over(w5))
    .withColumn("quantity_avg_2_prior", F.avg("quantity").over(w6))
    .withColumn("cumulative_quantity_whole_2", F.sum("quantity_whole").over(w7))
)

metrics = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w8).alias("first_known_quantity_adjusted_bucket"),
        F.count("cumulative_quantity_whole").over(w9).alias("store_id_count_cumulative_quantity_whole"),
        F.ntile(4).over(w10).alias("cumulative_quantity_whole_bucket"),
        F.first("cumulative_quantity_whole", ignorenulls=True).over(w11).alias("first_known_cumulative_quantity_whole"),
    )
)

totals = (
    metrics
    .groupBy("store_id")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("first_known_quantity_adjusted", [0.25, 0.5, 0.75]).alias("first_known_quantity_adjusted_quartiles"),
    )
    .filter(F.col("quantity_bit_and") >= 1)
)

result = (
    totals
    .select("store_id", "quantity_bit_and", "first_known_quantity_adjusted_quartiles")
    .orderBy(F.col("store_id").asc_nulls_first(), F.col("quantity_bit_and").desc())
    .limit(10)
)
