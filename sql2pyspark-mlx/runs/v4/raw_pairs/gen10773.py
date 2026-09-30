from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("sqft").desc())
w4 = Window.orderBy(F.col("sqft_dense_rank_2").desc())
w5 = Window.partitionBy("region", "city").orderBy("running_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w7 = Window.partitionBy("city").orderBy("sqft_dense_rank_2_rank", "id")
w8 = Window.partitionBy("city").orderBy(F.col("sqft_dense_rank_2").desc())
w9 = Window.partitionBy("city")
w10 = Window.partitionBy("region", "city").orderBy("event_seq", "id").rowsBetween(-6, Window.currentRow)
w11 = Window.partitionBy("region").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_stores
    .select(
        "*",
        F.dense_rank().over(w).alias("sqft_dense_rank"),
        F.sum("sqft").over(w2).alias("running_sqft"),
        F.dense_rank().over(w3).alias("sqft_dense_rank_2"),
    )
)

scored = (
    with_windows
    .withColumn("sqft_dense_rank_2_rank", F.rank().over(w4))
    .withColumn("first_known_sqft_dense_rank", F.first("sqft_dense_rank", ignorenulls=True).over(w5))
)

scored_stores = (
    scored
    .select(
        "*",
        F.row_number().over(w6).alias("event_seq"),
        F.row_number().over(w7).alias("row_seq"),
        F.percent_rank().over(w8).alias("sqft_dense_rank_2_percent_rank"),
        F.round((F.col("running_sqft") / F.sum("running_sqft").over(w9)), 4).alias("running_sqft_share"),
    )
)

result = (
    scored_stores
    .select(
        "*",
        F.avg("first_known_sqft_dense_rank").over(w10).alias("first_known_sqft_dense_rank_avg_6"),
        F.sum("running_sqft_share").over(w11).alias("running_running_sqft_share"),
        F.sum("sqft_dense_rank_2_rank").over(w11).alias("running_sqft_dense_rank_2_rank"),
    )
)
