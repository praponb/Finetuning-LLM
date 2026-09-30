from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_first())
w_ordered = Window.partitionBy("product_id").orderBy(F.col("quantity_cume_dist").desc(), "id")
w4 = Window.partitionBy("product_id")
w_part = Window.partitionBy("product_id").orderBy("last_restock", "id")
w6 = Window.partitionBy("product_id").orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy(F.col("product_id_sum_next_quantity").desc(), "id")
w8 = Window.partitionBy("product_id").orderBy("prev_quantity_cume_dist", "id")
w9 = Window.partitionBy("product_id").orderBy(F.col("last_restock").asc_nulls_last(), "id")
w10 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("quantity_cume_dist").desc(), "id")
w12 = Window.orderBy("prev_quantity_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("quantity_cume_dist", "id")

joined = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.launch_date"), F.col("p.brand"), F.col("p.product_name"))
)

metrics = (
    joined
    .select(
        "id",
        "product_id",
        "last_restock",
        "launch_date",
        F.lead("quantity").over(w).alias("next_quantity"),
        F.row_number().over(w).alias("row_seq"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.sum("next_quantity").over(w4).alias("product_id_sum_next_quantity"),
        F.lag("quantity_cume_dist", 2).over(w_part).alias("prev_quantity_cume_dist"),
        F.sum("quantity_cume_dist").over(w6).alias("running_quantity_cume_dist"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.ntile(10).over(w7).alias("product_id_sum_next_quantity_bucket"),
        F.ntile(3).over(w8).alias("prev_quantity_cume_dist_bucket"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w9))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "next_quantity",
        "row_seq",
        "quantity_cume_dist",
        "product_id_sum_next_quantity",
        "prev_quantity_cume_dist",
        "running_quantity_cume_dist",
    )
)

metrics_inventory = (
    deduped
    .withColumn("running_row_seq", F.sum("row_seq").over(w10))
    .withColumn("event_seq", F.row_number().over(w11))
    .withColumn("first_known_product_id_sum_next_quantity", F.first("product_id_sum_next_quantity", ignorenulls=True).over(w12))
    .withColumn("event_seq_2", F.row_number().over(w13))
)

cleaned = (
    metrics_inventory
    .filter(F.col("event_seq").between(5, 8))
    .select(
        "*",
        F.least("product_id_sum_next_quantity", "first_known_product_id_sum_next_quantity").alias("least_product_id_sum_next_quantity_first_known_product_id_sum_next_quantity"),
    )
)

result = (
    cleaned
    .cube(
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.max("event_seq").alias("max_event_seq"),
        F.max("prev_quantity_cume_dist").alias("max_prev_quantity_cume_dist"),
        F.percentile_approx("least_product_id_sum_next_quantity_first_known_product_id_sum_next_quantity", 0.95).alias("least_product_id_sum_next_quantity_first_known_product_id_sum_next_quantity_p95"),
        F.percentile_approx("next_quantity", 0.9).alias("next_quantity_p90"),
        F.max("first_known_product_id_sum_next_quantity").alias("max_first_known_product_id_sum_next_quantity"),
        F.avg("running_row_seq").alias("avg_running_row_seq"),
    )
    .orderBy(F.col("next_quantity_p90").asc_nulls_first())
)
