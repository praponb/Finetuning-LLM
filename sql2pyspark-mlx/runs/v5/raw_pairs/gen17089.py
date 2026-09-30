from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("last_restock", "id")
w_part = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("cumulative_quantity", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("cumulative_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("seq_no", "id")
w6 = Window.orderBy(F.col("seq_no_avg_2").desc_nulls_first())
w7 = Window.orderBy(F.col("next_quantity").desc_nulls_first())

scored = (
    df_inventory
    .select(
        "id",
        "store_id",
        F.lead("quantity").over(w_hist).alias("next_quantity"),
        F.lag("quantity").over(w_hist).alias("prev_quantity"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.sum("quantity").over(w_part).alias("cumulative_quantity"),
    )
)

scored_inventory = (
    scored
    .filter(F.col("store_id").isin([1, 5, 6]))
    .select(
        "id",
        "next_quantity",
        "prev_quantity",
        "cumulative_quantity",
        F.avg("seq_no").over(w3).alias("seq_no_avg_2"),
        F.last("next_quantity", ignorenulls=True).over(w4).alias("last_known_next_quantity"),
        F.lead("next_quantity", 1).over(w5).alias("next_next_quantity"),
    )
)

remaining_keys = (
    scored_inventory
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([5, 7, 10]))
        .select("id")
    )
)

reconciled = (
    scored_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("seq_no_avg_2_cume_dist", F.cume_dist().over(w6))
    .withColumn("next_quantity_rank", F.rank().over(w7))
)

valid = (
    with_windows
    .filter(F.col("next_quantity") != 110)
    .select(
        "*",
        (F.col("cumulative_quantity") - F.col("seq_no_avg_2_cume_dist")).alias("cumulative_quantity_minus_seq_no_avg_2_cume_dist"),
        (F.col("next_next_quantity") + F.col("next_quantity_rank")).alias("next_next_quantity_plus_next_quantity_rank"),
    )
)

totals = (
    valid
    .cube(
        F.when(F.col("cumulative_quantity_minus_seq_no_avg_2_cume_dist") >= 69, "LARGE").otherwise("SMALL").alias("cumulative_quantity_minus_seq_no_avg_2_cume_dist_tier"),
        F.when(F.col("next_quantity") >= 214, "LARGE").otherwise("SMALL").alias("next_quantity_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("seq_no_avg_2_cume_dist"), 2).alias("avg_seq_no_avg_2_cume_dist"),
    )
    .filter(F.col("avg_seq_no_avg_2_cume_dist") > 0)
)

result = totals.select("next_quantity_tier", "num_rows", "avg_seq_no_avg_2_cume_dist")
