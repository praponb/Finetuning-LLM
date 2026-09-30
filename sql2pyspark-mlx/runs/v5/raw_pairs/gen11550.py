from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost_scaled", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id")
w3 = Window.partitionBy("order_id", "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost_scaled").asc_nulls_last())
w5 = Window.partitionBy("status")
w6 = Window.partitionBy("carrier").orderBy(F.col("cost_share").desc(), "id")
w7 = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_first())
w8 = Window.partitionBy("carrier").orderBy("cost_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("cost_scaled_avg_5_prior", "id")
w10 = Window.orderBy("cost_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("shipped_date").desc(), "id")
w12 = Window.orderBy("cost_share", "id").rowsBetween(-5, -1)
w13 = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.orderBy("cost_scaled_dense_rank_filled", "id")
w_hist = Window.orderBy(F.col("cost_scaled_dense_rank_filled").desc(), "id")

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .withColumn("status_carrier", F.concat("status", F.lit(", "), "carrier"))
)

valid = (
    base
    .filter(F.col("delivered_date") > F.lit("2023-04-15").cast("date"))
    .select("*", F.round((F.col("cost") * 1.15), 2).alias("cost_scaled"))
)

enriched = (
    valid
    .select(
        "*",
        F.avg("cost_scaled").over(w).alias("cost_scaled_avg_5_prior"),
        F.lead("cost", 2, 0).over(w2).alias("next_cost"),
        F.sum("cost_scaled").over(w3).alias("running_cost_scaled"),
        F.dense_rank().over(w4).alias("cost_scaled_dense_rank"),
        F.round((F.col("cost") / F.sum("cost").over(w5)), 4).alias("cost_share"),
    )
)

valid_shipments = (
    enriched
    .filter(F.col("delivered_date") > F.lit("2023-03-19").cast("date"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status_carrier",
        "cost_scaled",
        "cost_scaled_avg_5_prior",
        "running_cost_scaled",
        "cost_share",
        F.coalesce("cost_scaled_dense_rank", F.lit(0)).alias("cost_scaled_dense_rank_filled"),
    )
)

deduped = (
    valid_shipments
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "shipped_date",
        "cost_scaled",
        "cost_share",
        "cost_scaled_dense_rank_filled",
        "row_num",
        F.dense_rank().over(w7).alias("cost_dense_rank"),
        F.sum("cost_scaled").over(w8).alias("running_cost_scaled_2"),
        F.row_number().over(w9).alias("row_seq"),
        F.sum("cost_scaled").over(w10).alias("running_cost_scaled_3"),
        F.sum("running_cost_scaled").over(w8).alias("cumulative_running_cost_scaled"),
    )
)

metrics_shipments = (
    metrics
    .select(
        "id",
        "shipped_date",
        "cost_scaled",
        "cost_share",
        "cost_scaled_dense_rank_filled",
        "row_num",
        "cost_dense_rank",
        "running_cost_scaled_2",
        "cumulative_running_cost_scaled",
        F.row_number().over(w11).alias("row_seq_2"),
        F.avg("cost_share").over(w12).alias("cost_share_avg_5_prior"),
        F.sum("cost_scaled").over(w13).alias("cumulative_cost_scaled"),
        F.lag("cost_share", 1, 0).over(w14).alias("prev_cost_share"),
        F.ntile(10).over(w_hist).alias("cost_scaled_dense_rank_filled_bucket"),
    )
)

result = (
    metrics_shipments
    .groupBy(
        F.date_trunc("YEAR", "shipped_date").alias("year_start"),
        F.when(F.col("row_seq_2") >= 9, "HIGH").otherwise("LOW").alias("row_seq_2_tier"),
    )
    .agg(
        F.percentile_approx("cost_scaled_dense_rank_filled_bucket", [0.25, 0.5, 0.75]).alias("cost_scaled_dense_rank_filled_bucket_quartiles"),
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
    )
)
