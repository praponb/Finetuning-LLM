from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_suppliers
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
        F.sum("rating").over(w3).alias("running_rating"),
        F.avg("rating").over(w4).alias("rating_avg_6"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("running_rating", 0.9).alias("running_rating_p90"),
    )
)
