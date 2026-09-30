from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status", "carrier").orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status", "carrier")
w3 = Window.orderBy("last4_sum_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier", "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("last4_sum_cost").desc_nulls_last())
w6 = Window.partitionBy("carrier")
w7 = Window.partitionBy("status", "order_id")

enriched = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED"]))
    .select(
        "*",
        F.avg("cost").over(w_running).alias("last4_avg_cost"),
        F.sum("cost").over(w_running).alias("last4_sum_cost"),
        F.count("cost").over(w_running).alias("last4_count_cost"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.sum("last4_count_cost").over(w3).alias("cumulative_last4_count_cost"),
        F.first("carrier", ignorenulls=True).over(w4).alias("first_known_carrier"),
    )
)

kept_keys = (
    scored
    .select("carrier", "id")
    .intersect(
        df_shipments
        .filter(F.col("status") == "IN_TRANSIT")
        .select("carrier", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w5).alias("last4_sum_cost_dense_rank"),
        F.max("cumulative_last4_count_cost").over(w6).alias("carrier_max_cumulative_last4_count_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w6)), 4).alias("cost_share_2"),
        F.avg("cost_share").over(w7).alias("status_avg_cost_share"),
        F.round((F.col("last4_avg_cost") / F.sum("last4_avg_cost").over(w7)), 4).alias("last4_avg_cost_share"),
    )
)

valid = (
    metrics
    .filter(F.col("status").isin(["LOST", "RETURNED"]))
    .withColumn("last4_sum_cost_dense_rank_per_cost_share_2", (F.col("last4_sum_cost_dense_rank") / F.nullif("cost_share_2", F.lit(0))))
    .withColumn("cost_share_2_whole", F.floor("cost_share_2"))
)

result = (
    valid
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "last4_avg_cost",
        "last4_sum_cost",
        "cost_share",
    )
    .orderBy(F.col("shipped_date").desc_nulls_first())
)
