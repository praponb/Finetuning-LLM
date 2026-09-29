from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("carrier_length").asc_nulls_first())
w2 = Window.orderBy(F.col("shipped_date").desc(), "id")
w_grp = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("carrier_length", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("carrier")
w6 = Window.partitionBy("carrier", "order_id").orderBy(F.col("delivered_date").desc(), "id")
w_hist = Window.orderBy("cost_bucket", "id")
w8 = Window.orderBy(F.col("cost_bucket_bucket").desc(), "id")
w9 = Window.orderBy("seq_no_2", "id")
w10 = Window.partitionBy("carrier").orderBy(F.col("cost_bucket_bucket").desc_nulls_first(), "id")
w11 = Window.partitionBy("carrier").orderBy("cost_bucket_bucket_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("carrier").orderBy(F.col("running_sum_seq_no_avg_3_prior").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-06-23").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.length("carrier").alias("carrier_length"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

with_windows = (
    cleaned
    .withColumn("carrier_length_cume_dist", F.cume_dist().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("cost_bucket", F.ntile(10).over(w_grp))
)

with_windows_shipments = (
    with_windows
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "seq_no",
        F.avg("seq_no").over(w4).alias("seq_no_avg_3_prior"),
        F.sum("cost").over(w5).alias("carrier_sum_cost"),
        F.row_number().over(w6).alias("seq_no_2"),
        F.ntile(3).over(w_hist).alias("cost_bucket_bucket"),
    )
)

with_windows_shipments_2 = (
    with_windows_shipments
    .select(
        "*",
        F.ntile(5).over(w8).alias("cost_bucket_bucket_bucket"),
        F.ntile(4).over(w9).alias("seq_no_2_bucket"),
    )
)

deduped = (
    with_windows_shipments_2
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "carrier",
        "seq_no",
        "seq_no_avg_3_prior",
        "cost_bucket_bucket_bucket",
        "seq_no_2_bucket",
    )
)

metrics = (
    deduped
    .select(
        "*",
        F.sum("seq_no_avg_3_prior").over(w11).alias("running_sum_seq_no_avg_3_prior"),
        F.min("seq_no_avg_3_prior").over(w11).alias("running_min_seq_no_avg_3_prior"),
        F.avg("seq_no_avg_3_prior").over(w11).alias("running_avg_seq_no_avg_3_prior"),
        F.max("seq_no_avg_3_prior").over(w11).alias("running_max_seq_no_avg_3_prior"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w12))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "seq_no",
        "seq_no_avg_3_prior",
        "running_avg_seq_no_avg_3_prior",
        "running_max_seq_no_avg_3_prior",
        "latest_rank",
    )
)
