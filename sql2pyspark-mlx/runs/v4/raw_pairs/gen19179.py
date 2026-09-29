from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("carrier").orderBy(F.col("status_length").asc_nulls_last())
w3 = Window.orderBy(F.col("status_length_avg_3_prior").desc_nulls_first())
w4 = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last())
w_running = Window.partitionBy("order_id").orderBy("status_length", "id")
w6 = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_last())
w7 = Window.partitionBy("carrier", "status")
w8 = Window.partitionBy("carrier").orderBy("prev_status_length_dense_rank", "id")
w9 = Window.orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("order_id").orderBy("status_length_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("order_id")
w12 = Window.partitionBy("carrier", "order_id").orderBy(F.col("seq_no").desc(), "id")

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("carrier").isin(["Kerry", "FedEx", "UPS"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.date_add("shipped_date", 87).alias("shipped_date_plus_87d"),
        F.length("status").alias("status_length"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.avg("status_length").over(w).alias("status_length_avg_3_prior"),
        F.dense_rank().over(w2).alias("status_length_dense_rank"),
    )
)

metrics_shipments = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w3).alias("status_length_avg_3_prior_dense_rank"),
        F.rank().over(w4).alias("cost_rank"),
        F.lag("status_length_dense_rank", 1, 0).over(w_running).alias("prev_status_length_dense_rank"),
        F.dense_rank().over(w6).alias("cost_dense_rank"),
    )
)

metrics_shipments_2 = (
    metrics_shipments
    .withColumn("carrier_count_cost", F.count("cost").over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("status_length_avg_3_prior_dense_rank_avg_3", F.avg("status_length_avg_3_prior_dense_rank").over(w9))
    .withColumn("cumulative_prev_status_length_dense_rank", F.sum("prev_status_length_dense_rank").over(w10))
    .withColumn("order_id_max_status_length", F.max("status_length").over(w11))
)

deduped = (
    metrics_shipments_2
    .withColumn("row_num", F.row_number().over(w12))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        "status_length",
        "status_length_avg_3_prior",
        "status_length_dense_rank",
        "status_length_avg_3_prior_dense_rank",
        "cost_dense_rank",
        F.when(F.col("status").rlike("^(DELIVERED|LOST)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("cost_dense_rank").asc_nulls_last())
)
