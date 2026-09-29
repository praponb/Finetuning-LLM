from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("avg_cost").desc())

valid = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("cost") >= 88, "High").when(F.col("cost") >= 45, "Medium").otherwise("Low").alias("cost_tier"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("activity_month"), "status")
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("cost_per_delivered"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles_2"),
    )
    .withColumn("avg_cost_rank", F.rank().over(w))
    .orderBy("cost_per_delivered")
)
