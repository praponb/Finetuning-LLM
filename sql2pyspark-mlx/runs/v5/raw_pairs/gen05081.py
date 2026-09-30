from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_suppliers
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("running_rating", F.sum("rating").over(w))
    .withColumn("rating_avg_5", F.avg("rating").over(w3))
    .withColumn("cumulative_rating", F.sum("rating").over(w4))
)

result = (
    scored
    .cube("city", "country")
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.sum("cumulative_rating") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("cumulative_rating_per_berlin"),
        F.grouping_id().alias("grouping_level"),
    )
)
