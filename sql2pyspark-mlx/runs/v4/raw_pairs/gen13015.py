from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").asc_nulls_last())
w2 = Window.partitionBy("carrier")
w3 = Window.orderBy(F.col("cost").desc())
w4 = Window.partitionBy("status")
w5 = Window.orderBy(F.col("cost_share").desc_nulls_last())
w6 = Window.partitionBy("order_id")
w7 = Window.partitionBy("carrier", "status")
w8 = Window.partitionBy("order_status")
w9 = Window.partitionBy("order_id").orderBy(F.col("carrier_avg_cost").desc_nulls_first())

kept_keys = (
    df_shipments
    .select("order_id", "id")
    .intersect(
        df_shipments
        .filter(F.col("carrier") == "Kerry")
        .select("order_id", "id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.avg("cost").over(w2).alias("carrier_avg_cost"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.dense_rank().over(w3).alias("cost_dense_rank_2"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.status").alias("order_status"), F.col("p.order_date"))
)

scored_shipments = (
    enriched
    .withColumn("status_count_cost", F.count("cost").over(w4))
    .withColumn("cost_share_percent_rank", F.percent_rank().over(w5))
    .withColumn("order_id_sum_cost_share", F.sum("cost_share").over(w6))
)

with_windows = (
    scored_shipments
    .select(
        "*",
        F.max("status_count_cost").over(w2).alias("carrier_max_status_count_cost"),
        F.max("cost_dense_rank_2").over(w4).alias("status_max_cost_dense_rank_2"),
        F.avg("cost_share").over(w6).alias("order_id_avg_cost_share"),
    )
)

scored_shipments_2 = (
    with_windows
    .select(
        "*",
        F.round((F.col("cost_dense_rank") / F.sum("cost_dense_rank").over(w7)), 4).alias("cost_dense_rank_share"),
        F.avg("cost_share").over(w8).alias("order_status_avg_cost_share"),
        F.percent_rank().over(w9).alias("carrier_avg_cost_percent_rank"),
    )
)

result = (
    scored_shipments_2
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        "cost_dense_rank",
        "carrier_avg_cost",
        "cost_share",
        "order_status",
    )
)
