from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_suppliers
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.lead("rating", 1, 0).over(w3).alias("next_rating"),
        F.avg("rating").over(w4).alias("rating_avg_3"),
    )
)

result = (
    with_windows
    .groupBy("country")
    .agg(
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.min("rating_avg_3").alias("min_rating_avg_3"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
    )
    .filter(F.col("new_york_rows") > 1)
    .orderBy(F.col("berlin_events").desc_nulls_first())
)
