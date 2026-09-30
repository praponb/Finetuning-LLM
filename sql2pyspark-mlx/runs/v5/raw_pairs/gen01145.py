from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("carrier", "status").orderBy(F.col("cost").desc(), "id")
w3 = Window.orderBy("running_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier", "status").orderBy("event_seq", "id")
w5 = Window.orderBy("event_seq", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("carrier").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("carrier")
w8 = Window.partitionBy("carrier").orderBy("next_cost", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("carrier").orderBy("next_cost", "id")
w10 = Window.orderBy(F.col("next_cost").asc_nulls_last())

scored = (
    df_shipments
    .filter(F.col("shipped_date") <= "2023-06-01")
    .withColumn("running_cost", F.sum("cost").over(w))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)

filtered = (
    scored
    .filter(F.col("event_seq") > 6)
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "running_cost",
        "event_seq",
        F.concat_ws("-", "status", "carrier").alias("status_carrier_key"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "carrier",
        "event_seq",
        "status_carrier_key",
        F.sum("cost").over(w3).alias("cumulative_cost"),
        F.lead("cost", 1).over(w4).alias("next_cost"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("next_cost").over(w5).alias("next_cost_avg_5"),
        F.first("cumulative_cost", ignorenulls=True).over(w6).alias("first_known_cumulative_cost"),
        F.count("next_cost").over(w7).alias("carrier_count_next_cost"),
        F.avg("next_cost").over(w8).alias("next_cost_avg_5_2"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "carrier",
        "event_seq",
        "status_carrier_key",
        "cumulative_cost",
        "next_cost_avg_5",
        "carrier_count_next_cost",
        F.stddev_samp("carrier_count_next_cost").over(w7).alias("carrier_stddev_samp_carrier_count_next_cost"),
        F.row_number().over(w9).alias("row_seq"),
        F.cume_dist().over(w10).alias("next_cost_cume_dist"),
    )
)

result = (
    enriched
    .withColumn("carrier_count_next_cost_tier", F.when(F.col("carrier_count_next_cost") >= 13, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["carrier", "carrier_count_next_cost_tier"], ["carrier"], []],
        "carrier",
        "carrier_count_next_cost_tier",
    )
    .agg(
        F.sum("next_cost_avg_5").alias("total_next_cost_avg_5"),
        F.countDistinct("carrier").alias("distinct_carriers"),
    )
    .orderBy("carrier")
)
