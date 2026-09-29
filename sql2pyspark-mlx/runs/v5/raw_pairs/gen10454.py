from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status", "order_id").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("cost")
w4 = Window.partitionBy("status").orderBy(F.col("cost_percent_rank").desc_nulls_first(), "id")
w5 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w_running = Window.partitionBy("carrier", "status").orderBy("cost_share", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("carrier")

filtered = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("Flash"))) & F.col("order_id").isNotNull())
    .withColumn("cost_tier", F.when(F.col("cost") >= 97, "High").when(F.col("cost") >= 14, "Medium").otherwise("Low"))
)

with_windows = (
    filtered
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_tier",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.avg("cost").over(w2).alias("cost_avg_6"),
        F.percent_rank().over(w3).alias("cost_percent_rank"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

base = (
    deduped
    .filter((F.col("delivered_date") > F.lit("2023-04-15").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("cost_share_tier", F.when(F.col("cost_share") >= 0.82, "High").when(F.col("cost_share") >= 0.35, "Medium").otherwise("Low"))
    .withColumn("status_lower", F.lower("status"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("UNKNOWN")))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost_share",
        "cost_avg_6",
        "rn",
        "cost_share_tier",
        "latest_rank",
        F.sum("cost_percent_rank").over(w_running).alias("last7_sum_cost_percent_rank"),
        F.avg("cost_percent_rank").over(w_running).alias("last7_avg_cost_percent_rank"),
        F.count("cost_percent_rank").over(w_running).alias("last7_count_cost_percent_rank"),
        F.max("cost_percent_rank").over(w_running).alias("last7_max_cost_percent_rank"),
        F.sum("cost_share").over(w7).alias("carrier_sum_cost_share"),
    )
)
