from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("city").orderBy(F.col("sqft_bucket_2").desc_nulls_first(), "id")
w4 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last(), "id")
w_recent = Window.partitionBy("region", "city").orderBy("sqft", "id")
w6 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc_nulls_first())
w7 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("region", "city")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w10 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, -1)
w11 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("city")
w13 = Window.partitionBy("region").orderBy("sqft_bucket_2_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_stores
    .select(
        "*",
        F.ntile(5).over(w).alias("sqft_bucket"),
        F.ntile(10).over(w2).alias("sqft_bucket_2"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group_2", F.row_number().over(w4))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

metrics = (
    latest
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w6))
    .withColumn("sqft_bucket_2_avg_5", F.avg("sqft_bucket_2").over(w7))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("sqft_bucket_2_share", F.round((F.col("sqft_bucket_2") / F.sum("sqft_bucket_2").over(w8)), 4))
)

kept = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.opened_date")), "left_semi")
)

with_latest = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel"))
)

result = (
    with_latest
    .select(
        "*",
        F.avg("sqft").over(w10).alias("sqft_avg_2_prior"),
        F.sum("sqft").over(w11).alias("cumulative_sqft"),
        F.round((F.col("sqft_bucket_2_avg_5") / F.sum("sqft_bucket_2_avg_5").over(w12)), 4).alias("sqft_bucket_2_avg_5_share"),
        F.first("sqft", ignorenulls=True).over(w13).alias("first_known_sqft"),
    )
)
