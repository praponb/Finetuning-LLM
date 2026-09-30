from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_first())
w2 = Window.partitionBy("carrier")
w_recent = Window.partitionBy("order_id", "status").orderBy(F.col("cost").asc_nulls_first())
w_ordered = Window.orderBy(F.col("cost").desc_nulls_last())
w_part = Window.partitionBy("carrier").orderBy(F.col("cost_percent_rank").desc_nulls_last())
w_hist = Window.partitionBy("order_id")
w7 = Window.partitionBy("carrier").orderBy(F.col("cost_cume_dist_2").desc())
w8 = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost_cume_dist").desc_nulls_first())
w9 = Window.orderBy("cost_percent_rank")
w10 = Window.orderBy(F.col("order_id_count_cost").desc_nulls_last())
w11 = Window.orderBy(F.col("cost_percent_rank").asc_nulls_last())

remaining_keys = (
    df_shipments
    .select("id")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date").between("2024-03-15", "2025-09-07"))
        .select("id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.cume_dist().over(w_running).alias("cost_cume_dist"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.dense_rank().over(w_recent).alias("cost_dense_rank"),
        F.percent_rank().over(w_ordered).alias("cost_percent_rank"),
        F.cume_dist().over(w_running).alias("cost_cume_dist_2"),
    )
)

scored = (
    metrics
    .select(
        "cost",
        "cost_share",
        "cost_percent_rank",
        F.rank().over(w_part).alias("cost_percent_rank_rank"),
        F.count("cost").over(w_hist).alias("order_id_count_cost"),
        F.min("cost_cume_dist").over(w2).alias("carrier_min_cost_cume_dist"),
        F.dense_rank().over(w7).alias("cost_cume_dist_2_dense_rank"),
        F.cume_dist().over(w8).alias("cost_cume_dist_cume_dist"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w9).alias("cost_percent_rank_dense_rank"),
        F.dense_rank().over(w10).alias("order_id_count_cost_dense_rank"),
        F.rank().over(w11).alias("cost_percent_rank_rank_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("cost_percent_rank_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("cost_percent_rank_dense_rank_tier"),
        F.when(F.col("order_id_count_cost") >= 13, "LARGE").otherwise("SMALL").alias("order_id_count_cost_tier"),
    )
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
    )
    .filter(F.col("cnt_2") >= 1)
    .orderBy("cost_percent_rank_dense_rank_tier")
)
