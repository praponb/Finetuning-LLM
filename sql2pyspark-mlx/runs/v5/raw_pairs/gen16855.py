from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w_hist = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("cost_share", "id")
w_part = Window.orderBy("greatest_last_known_next_cost_seq_no", "id")
w7 = Window.orderBy(F.col("seq_no").desc(), "id")
w_ordered = Window.orderBy(F.col("greatest_last_known_next_cost_seq_no").desc(), "id")
w9 = Window.orderBy("prev_seq_no", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.orderBy("order_id_min_last_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("next_seq_no", "id")

deduped = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "shipped_date", "delivered_date", "cost")
)

with_windows = (
    deduped
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.lead("cost", 1).over(w3).alias("next_cost"),
        F.min("cost").over(w2).alias("order_id_min_cost"),
        F.last("cost", ignorenulls=True).over(w_hist).alias("last_known_cost"),
        F.first("cost", ignorenulls=True).over(w_hist).alias("first_known_cost"),
    )
)

with_windows_shipments = (
    with_windows
    .select(
        "id",
        "shipped_date",
        "cost_share",
        "order_id_min_cost",
        F.last("next_cost", ignorenulls=True).over(w_hist).alias("last_known_next_cost"),
        F.min("last_known_cost").over(w2).alias("order_id_min_last_known_cost"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

filtered = (
    with_windows_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-09-12"))) & F.col("order_id_min_last_known_cost").isNull())
    .withColumn("greatest_last_known_next_cost_seq_no", F.greatest("last_known_next_cost", "seq_no"))
)

scored = (
    filtered
    .filter(F.col("cost_share") > 0.28)
    .withColumn("prev_seq_no", F.lag("seq_no").over(w_part))
    .withColumn("next_seq_no", F.lead("seq_no").over(w_part))
    .withColumn("prev_order_id_min_last_known_cost", F.lag("order_id_min_last_known_cost").over(w_part))
    .withColumn("seq_no_2", F.row_number().over(w_part))
)

with_windows_shipments_2 = (
    scored
    .withColumn("seq_no_3", F.row_number().over(w7))
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("greatest_last_known_next_cost_seq_no_avg_5", F.avg("greatest_last_known_next_cost_seq_no").over(w9))
    .withColumn("running_prev_seq_no", F.sum("prev_seq_no").over(w10))
    .withColumn("next_seq_no_bucket", F.ntile(10).over(w11))
)

filtered_shipments = (
    with_windows_shipments_2
    .filter(F.col("shipped_date") >= F.to_date(F.lit("2023-09-17")))
    .select(
        "*",
        F.col("last_known_next_cost").cast("int").alias("last_known_next_cost_int"),
        F.when(F.col("row_seq") >= 7, "High").when(F.col("row_seq") >= 3, "Medium").otherwise("Low").alias("row_seq_level"),
    )
)

result = (
    filtered_shipments
    .select(
        "id",
        "shipped_date",
        "cost_share",
        "order_id_min_cost",
        "last_known_next_cost",
        "order_id_min_last_known_cost",
        "seq_no",
        "greatest_last_known_next_cost_seq_no",
        "prev_seq_no",
    )
    .orderBy(F.col("shipped_date").desc_nulls_first(), "last_known_next_cost")
)
