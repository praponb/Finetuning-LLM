from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.avg("cost").over(w_part).alias("running_avg_cost"),
        F.sum("cost").over(w_part).alias("running_sum_cost"),
        F.max("cost").over(w_part).alias("running_max_cost"),
        F.min("cost").over(w_part).alias("running_min_cost"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("YEAR", "shipped_date").alias("year_start"),
        F.date_trunc("WEEK", "shipped_date").alias("week_start"),
    )
    .agg(
        F.sum("running_avg_cost").alias("total_running_avg_cost"),
        F.round(F.avg("running_sum_cost"), 2).alias("avg_running_sum_cost"),
        F.round(F.avg("running_max_cost"), 2).alias("avg_running_max_cost"),
        F.percentile_approx("running_min_cost", 0.95).alias("running_min_cost_p95"),
        F.round(F.avg("running_min_cost"), 2).alias("avg_running_min_cost"),
        F.round(F.avg("running_avg_cost"), 2).alias("avg_running_avg_cost"),
    )
    .orderBy(F.col("week_start").desc())
)
