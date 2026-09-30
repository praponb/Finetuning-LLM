from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status")
w_recent = Window.orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("status", "order_id").orderBy(F.col("carrier_count_cost").asc_nulls_first())
w4 = Window.orderBy(F.col("carrier_count_cost").desc())
w5 = Window.orderBy("carrier_count_cost")
w6 = Window.partitionBy("order_id")

remaining_keys = (
    df_shipments
    .select("carrier", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") < "2025-07-01")
        .select("carrier", "order_id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.year("shipped_date") >= 2025)
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "status",
        F.count("cost").over(w).alias("carrier_count_cost"),
        F.dense_rank().over(w_recent).alias("cost_dense_rank"),
    )
)

enriched = (
    metrics
    .select(
        "order_id",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_dense_rank",
        F.rank().over(w3).alias("carrier_count_cost_rank"),
        F.percent_rank().over(w4).alias("carrier_count_cost_percent_rank"),
        F.rank().over(w5).alias("carrier_count_cost_rank_2"),
        F.count("carrier_count_cost").over(w6).alias("order_id_count_carrier_count_cost"),
    )
)

remaining_keys_2 = (
    enriched
    .select("status", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("carrier") == "Kerry")
        .select("status", "order_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.order_id",
        "src.status",
        "src.cost_dense_rank",
        "src.carrier_count_cost_rank",
        "src.carrier_count_cost_percent_rank",
        "src.order_id_count_carrier_count_cost",
        F.col("b.order_date"),
    )
)

result = (
    joined
    .groupBy("order_id")
    .pivot("status", ["LOST", "IN_TRANSIT", "DELIVERED"])
    .agg(
        F.sum("order_id_count_carrier_count_cost").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
