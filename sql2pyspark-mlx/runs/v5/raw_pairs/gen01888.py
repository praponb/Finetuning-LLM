from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy("cost", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("shipped_date", "id")

enriched = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.avg("cost").over(w).alias("cost_avg_5_prior"),
        F.sum("cost").over(w_running).alias("running_cost"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.running_cost",
        F.col("p.status"),
        F.col("p.amount"),
        F.col("p.channel"),
    )
)

base = (
    joined
    .filter((F.col("status").eqNullSafe(F.lit("SHIPPED"))) & (F.col("amount") > 1120))
    .select(
        "id",
        "running_cost",
        "status",
        "amount",
        "channel",
        (F.col("amount") + F.col("running_cost")).alias("amount_plus_running_cost"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

base_shipments = (
    base
    .filter(F.col("amount") <= 930)
    .withColumn("running_cost_minus_amount", (F.col("running_cost") - F.col("amount")))
    .withColumn("status_filled", F.coalesce("status", F.lit("OTHER")))
)

result = (
    base_shipments
    .cube(
        "channel",
        F.when(F.col("running_cost") >= 518, "PREMIUM").otherwise("STANDARD").alias("running_cost_tier"),
    )
    .agg(
        F.percentile_approx("amount_plus_running_cost", 0.9).alias("amount_plus_running_cost_p90"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.round(F.sum("amount_plus_running_cost") / F.nullif(F.count(F.when(F.col("status") == "SHIPPED", 1)), F.lit(0)), 2).alias("amount_plus_running_cost_per_shipped"),
    )
)
