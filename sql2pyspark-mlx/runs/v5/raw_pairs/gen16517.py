from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w_hist = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc())
w4 = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.partitionBy("seq_no_tier").orderBy(F.col("new_york_events").desc())

enriched = (
    df_stores
    .select(
        "*",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.rank().over(w_hist).alias("sqft_rank"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("seq_no") >= 9, "HIGH").otherwise("LOW").alias("seq_no_tier"),
        "region",
    )
    .agg(
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.countDistinct("region").alias("distinct_regions"),
        F.countDistinct("region").alias("distinct_regions_2"),
    )
    .withColumn("new_york_events_dense_rank", F.dense_rank().over(w5))
    .orderBy(F.col("seq_no_tier").desc_nulls_last())
)
