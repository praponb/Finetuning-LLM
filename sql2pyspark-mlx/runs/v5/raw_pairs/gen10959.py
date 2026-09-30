from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("revenue", "id")
w3 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("units_tier").orderBy(F.col("units_avg_5_p95").desc())

with_windows = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.last("revenue", ignorenulls=True).over(w).alias("last_known_revenue"),
        F.lead("units", 1).over(w_grp).alias("next_units"),
        F.sum("units").over(w3).alias("running_units"),
        F.avg("units").over(w_recent).alias("units_avg_5"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("units") >= 30, "LARGE").otherwise("SMALL").alias("units_tier"),
        "store_id",
    )
    .agg(
        F.round(F.avg("units_avg_5"), 2).alias("avg_units_avg_5"),
        F.bit_or("units").alias("units_bit_or"),
        F.percentile_approx("units_avg_5", 0.95).alias("units_avg_5_p95"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
    )
    .withColumn("units_avg_5_p95_percent_rank", F.percent_rank().over(w5))
)
