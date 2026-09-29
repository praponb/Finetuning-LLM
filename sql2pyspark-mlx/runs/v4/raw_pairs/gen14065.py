from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("order_id").orderBy("order_id_min_cost", "id")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
    )
)

enriched = (
    valid
    .withColumn("running_days_delivered_date_to_shipped_date", F.sum("days_delivered_date_to_shipped_date").over(w))
    .withColumn("order_id_min_cost", F.min("cost").over(w2))
    .withColumn("last_known_days_delivered_date_to_shipped_date", F.last("days_delivered_date_to_shipped_date", ignorenulls=True).over(w3))
)

with_windows = (
    enriched
    .select(
        "*",
        F.stddev_samp("cost").over(w4).alias("status_stddev_samp_cost"),
        F.lead("last_known_days_delivered_date_to_shipped_date", 1).over(w5).alias("next_last_known_days_delivered_date_to_shipped_date"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("days_delivered_date_to_shipped_date") >= 80, "HIGH").otherwise("LOW").alias("days_delivered_date_to_shipped_date_tier"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.percentile_approx("order_id_min_cost", 0.5).alias("order_id_min_cost_p50"),
        F.sum(F.when(F.col("status") == "LOST", F.col("running_days_delivered_date_to_shipped_date")).otherwise(0)).alias("lost_running_days_delivered_date_to_shipped_date"),
    )
    .orderBy("order_id_min_cost_p50")
)
