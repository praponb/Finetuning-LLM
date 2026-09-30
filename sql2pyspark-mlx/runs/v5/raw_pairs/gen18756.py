from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity_rounded").desc_nulls_last())
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("unit_price").desc_nulls_first())
w4 = Window.orderBy("unit_price_dense_rank", "id")
w5 = Window.orderBy("last_known_unit_price", "id")
w6 = Window.orderBy(F.col("last_known_unit_price").desc(), "id")
w7 = Window.orderBy(F.col("running_unit_price").desc(), "id")
w_ordered = Window.orderBy("running_unit_price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("unit_price") < 300)
    .select("*", F.round("quantity", 0).alias("quantity_rounded"))
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "unit_price")
)

scored = (
    deduped
    .filter(F.col("unit_price") >= 270)
    .select(
        "id",
        "unit_price",
        F.last("unit_price", ignorenulls=True).over(w2).alias("last_known_unit_price"),
        F.sum("unit_price").over(w2).alias("cumulative_unit_price"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
        F.dense_rank().over(w3).alias("unit_price_dense_rank"),
    )
)

base = (
    scored
    .filter(F.col("unit_price_dense_rank") == 8)
    .select(
        "id",
        "unit_price",
        "last_known_unit_price",
        "running_unit_price",
        "unit_price_dense_rank",
        F.round((F.col("unit_price") / F.col("unit_price_dense_rank")), 2).alias("unit_price_per_unit_price_dense_rank"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("seq_no"),
        F.ntile(5).over(w6).alias("last_known_unit_price_bucket"),
        F.ntile(5).over(w7).alias("running_unit_price_bucket"),
    )
)

metrics_order_items = (
    metrics
    .select(
        "*",
        F.count("running_unit_price").over(w_ordered).alias("running_count_running_unit_price"),
        F.avg("running_unit_price").over(w_ordered).alias("running_avg_running_unit_price"),
        F.max("running_unit_price").over(w_ordered).alias("running_max_running_unit_price"),
        F.sum("unit_price").over(w_ordered).alias("cumulative_unit_price"),
    )
)

valid = (
    metrics_order_items
    .filter(F.col("running_avg_running_unit_price") >= 500)
    .select(
        "id",
        "unit_price",
        "last_known_unit_price",
        "unit_price_dense_rank",
        "unit_price_per_unit_price_dense_rank",
        "row_seq",
        "seq_no",
        "last_known_unit_price_bucket",
        "running_unit_price_bucket",
        "running_count_running_unit_price",
        "cumulative_unit_price",
        (F.col("running_unit_price") * F.col("seq_no")).alias("running_unit_price_times_seq_no"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

result = (
    valid
    .select(
        "id",
        "unit_price",
        "last_known_unit_price",
        "unit_price_dense_rank",
        "row_seq",
        "seq_no",
        "last_known_unit_price_bucket",
        "running_count_running_unit_price",
        "cumulative_unit_price",
    )
)
