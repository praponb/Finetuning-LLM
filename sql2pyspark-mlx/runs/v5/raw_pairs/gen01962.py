from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_suppliers
    .filter(F.col("rating") >= 3.9)
    .select(
        "id",
        "rating",
        F.last("city", ignorenulls=True).over(w).alias("last_known_city"),
        F.sum("rating").over(w2).alias("cumulative_rating"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("rating") >= 3.7, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.round(F.avg("cumulative_rating"), 2).alias("avg_cumulative_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("rating_p90") > 0)
    .orderBy("event_count")
)
