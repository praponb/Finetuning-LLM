from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("delivered_date").desc(), "id")
w_recent = Window.partitionBy("status").orderBy("latest_rank", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("carrier").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "order_id")

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") >= F.to_date(F.lit("2023-03-20")))
    .select("*", F.coalesce("cost", F.lit(0)).alias("cost_filled"))
)

joined = (
    cleaned
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id") == 4)
    .select("e.*", F.col("o.amount"))
)

latest = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        "cost_filled",
        "amount",
        "latest_rank",
        F.avg("cost_filled").over(w_recent).alias("cost_filled_avg_5_prior"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.count("cost_filled").over(w4).alias("status_count_cost_filled"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.status").alias("order_status"),
        F.col("dim.channel").alias("channel"),
        F.col("a.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.cost_filled_avg_5_prior")).alias("total_cost_filled_avg_5_prior"),
    )
    .orderBy(F.col("rows_total").desc())
)
