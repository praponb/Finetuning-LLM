from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w_part = Window.partitionBy("region", "store_id").orderBy("store_id_max_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region", "store_id").orderBy("running_count_prev_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("region").orderBy("store_id_max_quantity", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("region").orderBy(F.col("running_avg_prev_quantity_avg_3").asc_nulls_first(), "id")

valid = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 5]))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)

with_windows = (
    valid
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w))
    .withColumn("store_id_max_quantity", F.max("quantity").over(w2))
    .withColumn("next_quantity", F.lead("quantity", 1, 0).over(w3))
)

enriched = (
    with_windows
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.store_name").like("%Mall%"))
    .select(
        "s.id",
        "s.store_id",
        "s.quantity",
        "s.prev_quantity",
        "s.store_id_max_quantity",
        F.col("d.region"),
        F.col("d.store_name"),
    )
)

enriched_inventory = (
    enriched
    .withColumn("running_sum_prev_quantity", F.sum("prev_quantity").over(w_part))
    .withColumn("running_min_prev_quantity", F.min("prev_quantity").over(w_part))
    .withColumn("running_avg_prev_quantity", F.avg("prev_quantity").over(w_part))
    .withColumn("running_count_prev_quantity", F.count("prev_quantity").over(w_part))
)

enriched_inventory_2 = (
    enriched_inventory
    .select(
        "id",
        "store_id_max_quantity",
        "region",
        "running_avg_prev_quantity",
        "running_count_prev_quantity",
        F.sum("running_sum_prev_quantity").over(w5).alias("cumulative_running_sum_prev_quantity"),
        F.avg("running_avg_prev_quantity").over(w6).alias("running_avg_prev_quantity_avg_3"),
    )
)

deduped = (
    enriched_inventory_2
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "store_id_max_quantity",
        "running_avg_prev_quantity",
        "cumulative_running_sum_prev_quantity",
        "running_avg_prev_quantity_avg_3",
    )
)
