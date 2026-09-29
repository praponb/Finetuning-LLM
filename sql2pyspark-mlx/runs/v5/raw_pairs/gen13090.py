from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("prev_last_restock_period", "id")
w_grp = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("product_id", "store_id").orderBy("last_restock_period", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("session_no").desc(), "id")
w7 = Window.partitionBy("product_id").orderBy("quantity_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy("quantity", "id")
w9 = Window.partitionBy("product_id")
w10 = Window.partitionBy("store_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("store_id").orderBy("prev_last_restock_period", "id")
w12 = Window.partitionBy("store_id").orderBy("prev_last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.year("last_restock") == 2025)
    .select(
        "*",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

prev_events = base.withColumn("prev_last_restock_period", F.lag("last_restock_period").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_last_restock_period").isNull() | (F.col("last_restock_period") > F.col("prev_last_restock_period") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_inventory = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

with_windows = (
    sessions_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(5).over(w_grp).alias("quantity_bucket"),
        F.avg("quantity").over(w5).alias("quantity_avg_3"),
        F.ntile(10).over(w6).alias("session_no_bucket"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "prev_last_restock_period",
        "session_no",
        "seq_no",
        "session_no_bucket",
        F.sum("quantity").over(w7).alias("running_quantity"),
        F.lead("session_no_bucket", 1).over(w8).alias("next_session_no_bucket"),
        F.max("quantity_whole").over(w9).alias("product_id_max_quantity_whole"),
        F.sum("quantity").over(w10).alias("running_quantity_2"),
    )
)

prev_events_2 = (
    scored
    .withColumn("prev_prev_last_restock_period", F.lag("prev_last_restock_period").over(w11))
)

flagged = (
    prev_events_2
    .withColumn("is_new_session", F.when(F.col("prev_prev_last_restock_period").isNull() | (F.col("prev_last_restock_period") > F.col("prev_prev_last_restock_period") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "store_id",
        "last_restock",
        "prev_last_restock_period",
        "product_id_max_quantity_whole",
        "running_quantity_2",
        F.sum("is_new_session").over(w12).alias("burst_id"),
    )
)

filtered = (
    sessionized
    .filter(F.col("last_restock") >= F.lit("2023-09-15").cast("date"))
    .select(
        "id",
        "last_restock",
        "prev_last_restock_period",
        "running_quantity_2",
        "burst_id",
        (F.col("running_quantity_2") - F.col("product_id_max_quantity_whole")).alias("running_quantity_2_minus_product_id_max_quantity_whole"),
    )
)

result = (
    filtered
    .withColumn("week_start", F.date_trunc("week", "last_restock"))
    .groupingSets(
        [["burst_id", "week_start"], ["burst_id"], []],
        "burst_id",
        "week_start",
    )
    .agg(
        F.round(F.avg("running_quantity_2"), 2).alias("avg_running_quantity_2"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.max("prev_last_restock_period").alias("last_seen"),
        F.min("prev_last_restock_period").alias("first_prev_last_restock_period"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
)
