from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("cost", "id")
w3 = Window.partitionBy("status").orderBy(F.col("cost").asc_nulls_last())
w4 = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w_recent = Window.orderBy("cost", "id").rowsBetween(-2, -1)

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-10-08"))) & F.col("carrier").isNotNull())
    .withColumn("cost_rounded", F.round("cost", 0))
)

metrics = (
    filtered
    .withColumn("running_cost_rounded", F.sum("cost_rounded").over(w_hist))
    .withColumn("prev_cost", F.lag("cost", 2).over(w2))
    .withColumn("cost_cume_dist", F.cume_dist().over(w3))
)

cleaned = (
    metrics
    .filter(F.month("shipped_date").isin([6, 10, 12]))
    .withColumn("shipped_date_plus_months", F.add_months("shipped_date", 3))
)

valid = (
    cleaned
    .filter(F.col("order_id").isNotNull())
    .withColumn("is_delivered", F.when(F.col("status") == "DELIVERED", 1).otherwise(0))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("N/A")))
)

metrics_shipments = (
    valid
    .select(
        "*",
        F.lead("cost", 2, 0).over(w4).alias("next_cost"),
        F.avg("running_cost_rounded").over(w_recent).alias("running_cost_rounded_avg_2_prior"),
    )
)

result = (
    metrics_shipments
    .groupBy(
        F.when(F.col("prev_cost") >= 87, "HIGH").otherwise("LOW").alias("prev_cost_tier"),
        F.date_trunc("QUARTER", "shipped_date").alias("quarter_start"),
    )
    .agg(
        F.sum("next_cost").alias("total_next_cost"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("cost_per_lost"),
        F.count_if(F.col("status") == "IN_TRANSIT").alias("in_transit_events"),
    )
    .orderBy("quarter_start")
)
