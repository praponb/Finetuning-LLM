from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w_ordered = Window.partitionBy("carrier").orderBy("cost_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier")
w5 = Window.partitionBy("carrier").orderBy("cost_share_times_cost_share_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("carrier").orderBy(F.col("running_min_cumulative_cost").desc())
w7 = Window.partitionBy("carrier").orderBy("cost_share_times_cost_share_scaled", "id")

enriched = (
    df_shipments
    .filter(F.col("cost") >= 83)
    .select(
        "id",
        "carrier",
        "delivered_date",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

enriched_shipments = (
    enriched
    .select(
        "id",
        "carrier",
        "cost_share",
        F.avg("cumulative_cost").over(w_ordered).alias("running_avg_cumulative_cost"),
        F.min("cumulative_cost").over(w_ordered).alias("running_min_cumulative_cost"),
        F.sum("cumulative_cost").over(w_ordered).alias("running_sum_cumulative_cost"),
    )
)

valid = (
    enriched_shipments
    .filter(F.col("carrier").isin(["Flash", "UPS"]))
    .withColumn("cost_share_scaled", F.round((F.col("cost_share") * 1.1), 2))
)

cleaned = (
    valid
    .filter(~F.col("carrier").isin(["Flash", "FedEx", "UPS"]))
    .select(
        "id",
        "carrier",
        "running_avg_cumulative_cost",
        "running_min_cumulative_cost",
        "running_sum_cumulative_cost",
        "cost_share_scaled",
        (F.col("cost_share") * F.col("cost_share_scaled")).alias("cost_share_times_cost_share_scaled"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.round((F.col("running_min_cumulative_cost") / F.sum("running_min_cumulative_cost").over(w4)), 4).alias("running_min_cumulative_cost_share"),
        F.sum("running_sum_cumulative_cost").over(w5).alias("cumulative_running_sum_cumulative_cost"),
        F.percent_rank().over(w6).alias("running_min_cumulative_cost_percent_rank"),
        F.lead("running_sum_cumulative_cost", 2).over(w7).alias("next_running_sum_cumulative_cost"),
        F.stddev_samp("cost_share_times_cost_share_scaled").over(w4).alias("carrier_stddev_samp_cost_share_times_cost_share_scaled"),
    )
)

result = (
    with_windows
    .rollup(
        "carrier",
        F.when(F.col("carrier_stddev_samp_cost_share_times_cost_share_scaled") >= 78, "LARGE").otherwise("SMALL").alias("carrier_stddev_samp_cost_share_times_cost_share_scaled_tier"),
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cumulative_running_sum_cumulative_cost")).otherwise(0)).alias("dhl_cumulative_running_sum_cumulative_cost"),
        F.round(F.avg("cost_share_times_cost_share_scaled"), 2).alias("avg_cost_share_times_cost_share_scaled"),
    )
    .filter(F.col("avg_cost_share_times_cost_share_scaled") > 0)
    .orderBy("dhl_cumulative_running_sum_cumulative_cost")
)
