from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "carrier").orderBy("cost", "id")
w_grp = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("status_length").asc_nulls_last())
w5 = Window.partitionBy("order_id", "status")
w6 = Window.partitionBy("status").orderBy("status_length_dense_rank", "id")

base = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-08-07").cast("date"))
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.first("status_length", ignorenulls=True).over(w).alias("first_known_status_length"),
        F.lag("status_length", 1).over(w2).alias("prev_status_length"),
        F.sum("cost").over(w_grp).alias("cumulative_cost"),
        F.dense_rank().over(w4).alias("status_length_dense_rank"),
        F.sum("status_length").over(w5).alias("order_id_sum_status_length"),
    )
)

cleaned = (
    metrics
    .filter(F.col("shipped_date") >= "2025-07-19")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "status_length",
        "carrier_status_key",
        "carrier_clean",
        "first_known_status_length",
        "cumulative_cost",
        "status_length_dense_rank",
        "order_id_sum_status_length",
        (F.col("order_id_sum_status_length") * F.col("prev_status_length")).alias("order_id_sum_status_length_times_prev_status_length"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        "status_length",
        "carrier_status_key",
        "cumulative_cost",
        "status_length_dense_rank",
        "order_id_sum_status_length",
        "order_id_sum_status_length_times_prev_status_length",
        "carrier_filled",
    )
)

result = (
    deduped
    .withColumn("status_length_dense_rank_tier", F.when(F.col("status_length_dense_rank") >= 7, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["status", "status_length_dense_rank_tier"], ["status"]],
        "status",
        "status_length_dense_rank_tier",
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.min("status_length").alias("min_status_length"),
    )
    .orderBy("status")
)
