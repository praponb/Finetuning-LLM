from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").asc_nulls_last())
w2 = Window.partitionBy("carrier").orderBy("cost")
w3 = Window.orderBy("cost")
w4 = Window.partitionBy("status", "order_id").orderBy(F.col("cost").desc())
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("status").orderBy(F.col("cost_dense_rank").asc_nulls_last())
w8 = Window.orderBy(F.col("cost").desc_nulls_first())
w9 = Window.partitionBy("carrier")
w10 = Window.partitionBy("status", "order_id")
w11 = Window.partitionBy("status", "order_id").orderBy(F.col("order_id_avg_cost_share_2").desc_nulls_last())

base = (
    df_shipments
    .filter(F.year("shipped_date") == 2025)
    .withColumn("clean_carrier", F.trim("carrier"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("UNKNOWN")))
)

ranked = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

kept_keys = (
    ranked
    .select("order_id", "status")
    .intersectAll(
        df_shipments
        .filter(F.col("carrier") == "Flash")
        .select("order_id", "status")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.percent_rank().over(w2).alias("cost_percent_rank"),
        F.dense_rank().over(w3).alias("cost_dense_rank"),
        F.dense_rank().over(w4).alias("cost_dense_rank_2"),
        F.round((F.col("cost") / F.sum("cost").over(w5)), 4).alias("cost_share"),
        F.round((F.col("cost") / F.sum("cost").over(w6)), 4).alias("cost_share_2"),
    )
)

with_windows_shipments = (
    with_windows
    .withColumn("order_id_avg_cost_share_2", F.avg("cost_share_2").over(w5))
    .withColumn("status_count_cost_share_2", F.count("cost_share_2").over(w6))
    .withColumn("cost_dense_rank_percent_rank", F.percent_rank().over(w7))
    .withColumn("cost_percent_rank_2", F.percent_rank().over(w8))
)

enriched = (
    with_windows_shipments
    .select(
        "*",
        F.cume_dist().over(w3).alias("cost_cume_dist"),
        F.avg("cost_dense_rank_2").over(w9).alias("carrier_avg_cost_dense_rank_2"),
        F.count("cost_percent_rank").over(w10).alias("status_count_cost_percent_rank"),
        F.cume_dist().over(w11).alias("order_id_avg_cost_share_2_cume_dist"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.channel"))
)

grouped = (
    with_dims
    .groupBy("order_id")
    .agg(
        F.round(F.sum("cost_share") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("cost_share_per_delivered"),
        F.round(F.sum("cost_dense_rank_2") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("cost_dense_rank_2_per_store"),
        F.sum("cost_percent_rank").alias("total_cost_percent_rank"),
        F.max("order_id_avg_cost_share_2").alias("max_order_id_avg_cost_share_2"),
        F.percentile_approx("order_id_avg_cost_share_2", 0.9).alias("order_id_avg_cost_share_2_p90"),
        F.count_if(F.col("carrier") == "FedEx").alias("fedex_events"),
    )
    .filter(F.col("fedex_events") > 1)
)

result = (
    grouped
    .select(
        "order_id",
        "cost_dense_rank_2_per_store",
        "total_cost_percent_rank",
        "max_order_id_avg_cost_share_2",
        "fedex_events",
    )
)
