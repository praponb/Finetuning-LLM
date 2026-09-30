from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost_adjusted").desc_nulls_first())
w2 = Window.partitionBy("status")
w3 = Window.orderBy(F.col("cost_adjusted_rank").desc_nulls_first())
w4 = Window.partitionBy("order_id", "status").orderBy(F.col("cost_adjusted_rank").desc_nulls_first())
w5 = Window.partitionBy("order_id", "status")
w6 = Window.orderBy(F.col("cost_adjusted_rank_cume_dist").desc_nulls_last())
w7 = Window.partitionBy("status", "order_id")
w8 = Window.partitionBy("status", "order_id").orderBy(F.col("cost_adjusted").asc_nulls_first())

remaining_keys = (
    df_shipments
    .select("id")
    .exceptAll(
        df_shipments
        .filter(F.col("delivered_date") >= "2023-11-18")
        .select("id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-05-24"))) & (F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "order_id",
        "shipped_date",
        "status",
        F.round((F.col("cost") * 1.1), 2).alias("cost_adjusted"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("cost_adjusted_rank"),
        F.avg("cost_adjusted").over(w2).alias("status_avg_cost_adjusted"),
    )
)

with_windows_shipments = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w3).alias("cost_adjusted_rank_cume_dist"),
        F.cume_dist().over(w4).alias("cost_adjusted_rank_cume_dist_2"),
    )
)

metrics = (
    with_windows_shipments
    .select(
        "*",
        F.sum("cost_adjusted_rank_cume_dist").over(w5).alias("order_id_sum_cost_adjusted_rank_cume_dist"),
        F.dense_rank().over(w6).alias("cost_adjusted_rank_cume_dist_dense_rank"),
        F.max("cost_adjusted_rank_cume_dist").over(w7).alias("status_max_cost_adjusted_rank_cume_dist"),
        F.cume_dist().over(w8).alias("cost_adjusted_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "order_id",
        "status",
        "cost_adjusted",
        "carrier_clean",
        "cost_adjusted_rank",
        "cost_adjusted_rank_cume_dist",
        "cost_adjusted_rank_cume_dist_2",
        "cost_adjusted_rank_cume_dist_dense_rank",
        "status_max_cost_adjusted_rank_cume_dist",
    )
    .orderBy(
        F.col("cost_adjusted").desc_nulls_last(),
        "status_max_cost_adjusted_rank_cume_dist",
    )
)
