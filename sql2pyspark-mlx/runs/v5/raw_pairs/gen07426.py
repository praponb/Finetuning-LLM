from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("carrier")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w3 = Window.partitionBy("carrier").orderBy("next_carrier_min_cost", "id")
w4 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2023-04-14").cast("date"))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        F.when(F.col("cost") >= 84, "High").when(F.col("cost") >= 28, "Medium").otherwise("Low").alias("cost_level"),
        F.concat("carrier", F.lit("/"), "status").alias("carrier_status"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

valid_shipments = (
    valid
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-10-01")))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "cost_level",
        "carrier_status",
        "carrier_filled",
        F.when(F.col("carrier") == "UPS", 1).otherwise(0).alias("is_ups"),
        F.add_months("delivered_date", 1).alias("delivered_date_plus_months"),
    )
)

with_windows = (
    valid_shipments
    .filter(F.month("delivered_date").isin([1, 4, 6]))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost_level",
        F.count("is_ups").over(w_part).alias("carrier_count_is_ups"),
        F.min("cost").over(w_part).alias("carrier_min_cost"),
    )
)

enriched = (
    with_windows
    .withColumn("prev_carrier_count_is_ups", F.lag("carrier_count_is_ups").over(w2))
    .withColumn("next_carrier_count_is_ups", F.lead("carrier_count_is_ups").over(w2))
    .withColumn("next_carrier_min_cost", F.lead("carrier_min_cost").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
)

scored = (
    enriched
    .select(
        "id",
        "carrier",
        "cost_level",
        "carrier_min_cost",
        "next_carrier_min_cost",
        "event_seq",
        F.lead("next_carrier_count_is_ups").over(w3).alias("next_next_carrier_count_is_ups"),
        F.lag("next_carrier_min_cost").over(w3).alias("prev_next_carrier_min_cost"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

enriched_shipments = (
    scored
    .select(
        "id",
        "carrier",
        "carrier_min_cost",
        "prev_next_carrier_min_cost",
        F.sum("prev_next_carrier_min_cost").over(w_part).alias("carrier_sum_prev_next_carrier_min_cost"),
        F.sum("next_next_carrier_count_is_ups").over(w4).alias("running_next_next_carrier_count_is_ups"),
    )
)

result = (
    enriched_shipments
    .withColumn("prev_next_carrier_min_cost_tier", F.when(F.col("prev_next_carrier_min_cost") >= 84, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["prev_next_carrier_min_cost_tier", "carrier"], ["prev_next_carrier_min_cost_tier"]],
        "prev_next_carrier_min_cost_tier",
        "carrier",
    )
    .agg(
        F.percentile_approx("prev_next_carrier_min_cost", 0.5).alias("prev_next_carrier_min_cost_p50"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.count_if(F.col("carrier") == "Kerry").alias("kerry_events"),
        F.avg(F.when(F.col("carrier") == "Flash", F.col("carrier_sum_prev_next_carrier_min_cost"))).alias("flash_avg_carrier_sum_prev_next_carrier_min_cost"),
        F.count("*").alias("cnt"),
    )
    .orderBy("prev_next_carrier_min_cost_p50")
)
