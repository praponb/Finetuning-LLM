from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w4 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status")
w6 = Window.orderBy("adjusted_cost", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_shipments
    .select(
        "id",
        "order_id",
        "cost",
        F.row_number().over(w).alias("seq_no"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

base = (
    enriched
    .filter(F.col("seq_no") != 3)
    .select(
        "*",
        F.round((F.col("cost") * 0.9), 2).alias("adjusted_cost"),
        F.coalesce("cost_share", F.lit(0)).alias("cost_share_filled"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.store_id").isNotNull())
    .select(
        "src.id",
        "src.order_id",
        "src.cost",
        "src.cost_share",
        "src.adjusted_cost",
        F.col("b.status"),
        F.col("b.amount"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "order_id",
        "cost_share",
        "adjusted_cost",
        F.last("cost_share", ignorenulls=True).over(w4).alias("last_known_cost_share"),
        F.avg("cost").over(w5).alias("status_avg_cost"),
    )
)

base_shipments = (
    scored
    .filter(F.col("cost_share") < 0.42)
    .select(
        "id",
        "adjusted_cost",
        "last_known_cost_share",
        F.round((F.col("cost_share") / F.col("status_avg_cost")), 2).alias("cost_share_per_status_avg_cost"),
    )
)

scored_shipments = (
    base_shipments
    .select(
        "*",
        F.min("adjusted_cost").over(w6).alias("last7_min_adjusted_cost"),
        F.sum("adjusted_cost").over(w6).alias("last7_sum_adjusted_cost"),
        F.count("adjusted_cost").over(w6).alias("last7_count_adjusted_cost"),
    )
)

result = (
    scored_shipments
    .select(
        "id",
        "adjusted_cost",
        "last7_min_adjusted_cost",
        "last7_sum_adjusted_cost",
        "last7_count_adjusted_cost",
    )
)
