from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id")
w2 = Window.orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier", "order_id").orderBy("cost", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w6 = Window.partitionBy("status").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("cost_bucket").desc(), "id")
w_recent = Window.partitionBy("status").orderBy(F.col("seq_no").desc_nulls_last())
w9 = Window.partitionBy("status").orderBy(F.col("cost_avg_3_prior").desc_nulls_last())
w10 = Window.partitionBy("status")
w11 = Window.orderBy("cost_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy(F.col("cost_bucket").desc(), "id")

base = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-07-17"))) & (F.col("shipped_date") < "2023-03-28"))
    .select(
        "*",
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
        F.upper("carrier").alias("carrier_upper"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled_2"),
    )
)

scored = (
    base
    .filter(F.month("delivered_date").isin([3, 8, 11]))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_filled_2",
        F.lag("cost", 1).over(w).alias("prev_cost"),
        F.ntile(4).over(w2).alias("cost_bucket"),
        F.sum("cost").over(w3).alias("cumulative_cost"),
        F.avg("cost").over(w4).alias("cost_avg_3_prior"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

enriched = (
    scored
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w6))
    .withColumn("cost_bucket_bucket", F.ntile(3).over(w7))
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w_recent))
)

with_windows = (
    enriched
    .select(
        "id",
        "delivered_date",
        "carrier_filled_2",
        "prev_cost",
        "cost_bucket",
        "cumulative_cost",
        "cost_avg_3_prior",
        "seq_no",
        "first_known_cost",
        "cost_bucket_bucket",
        F.dense_rank().over(w9).alias("cost_avg_3_prior_dense_rank"),
        F.max("seq_no_dense_rank").over(w10).alias("status_max_seq_no_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.count("seq_no").over(w11).alias("running_count_seq_no"),
        F.avg("seq_no").over(w11).alias("running_avg_seq_no"),
        F.min("seq_no").over(w11).alias("running_min_seq_no"),
    )
)

base_shipments = (
    metrics
    .filter(F.col("running_count_seq_no") != 6)
    .select(
        "id",
        "delivered_date",
        "carrier_filled_2",
        "prev_cost",
        "cost_bucket",
        "cumulative_cost",
        "cost_avg_3_prior",
        "cost_bucket_bucket",
        "cost_avg_3_prior_dense_rank",
        "status_max_seq_no_dense_rank",
        "running_count_seq_no",
        "running_avg_seq_no",
        "running_min_seq_no",
        F.col("status_max_seq_no_dense_rank").cast("double").alias("status_max_seq_no_dense_rank_num"),
    )
)

result = (
    base_shipments
    .select(
        "id",
        "delivered_date",
        "prev_cost",
        "cost_bucket",
        "cost_avg_3_prior",
        "running_avg_seq_no",
        F.sum("cost_bucket").over(w12).alias("running_cost_bucket"),
        F.row_number().over(w13).alias("row_seq"),
    )
)
