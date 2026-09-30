from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy(F.col("quantity_num").desc(), "id")
w6 = Window.partitionBy("product_id").orderBy("quantity_num", "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("quantity_num").desc_nulls_first())
w8 = Window.orderBy(F.col("quantity").asc_nulls_last())

valid = (
    df_inventory
    .filter(F.col("product_id") == 2)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("double").alias("quantity_num"),
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)

metrics = (
    valid
    .withColumn("next_quantity", F.lead("quantity", 2).over(w))
    .withColumn("running_quantity", F.sum("quantity").over(w2))
    .withColumn("cumulative_quantity_num", F.sum("quantity_num").over(w3))
    .withColumn("running_quantity_num", F.sum("quantity_num").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

enriched = (
    metrics
    .withColumn("event_seq_2", F.row_number().over(w6))
    .withColumn("cumulative_running_quantity_num", F.sum("running_quantity_num").over(w4))
    .withColumn("quantity_num_percent_rank", F.percent_rank().over(w7))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w8))
)

daily = (
    enriched
    .groupBy("product_id")
    .agg(
        F.round(F.avg("running_quantity"), 2).alias("avg_running_quantity"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.avg("quantity_num_percent_rank"), 2).alias("avg_quantity_num_percent_rank"),
        F.round(F.avg("running_quantity_num"), 2).alias("avg_running_quantity_num"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
)

result = (
    daily
    .select(
        "product_id",
        "avg_running_quantity",
        "distinct_store_ids",
        "avg_quantity_num_percent_rank",
        "event_seq_quartiles",
    )
    .orderBy(F.col("avg_quantity_num_percent_rank").desc())
)
