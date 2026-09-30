from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city", "status").orderBy("opened_date", "id")
w4 = Window.orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy("status", "region").orderBy(F.col("row_seq_plus_prev_sqft").desc_nulls_last(), "id")
w6 = Window.orderBy("prev_sqft", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.orderBy("row_seq", "id")

deduped = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city", "region", "opened_date", "sqft")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel").alias("order_channel"), F.col("c.status"))
)

with_windows = (
    latest_match
    .withColumn("prev_sqft", F.lag("sqft").over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("sqft_bucket", F.ntile(4).over(w4))
)

eligible = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([1, 2, 7, 8])), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("opened_date") <= "2023-10-06")
    .select(
        "*",
        (F.col("row_seq") + F.col("prev_sqft")).alias("row_seq_plus_prev_sqft"),
        F.lower("order_channel").alias("order_channel_lower"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "opened_date",
        "prev_sqft",
        "row_seq",
        "sqft_bucket",
        "row_seq_plus_prev_sqft",
        "order_channel_lower",
    )
)

result = (
    latest
    .filter(F.col("row_seq") == 7)
    .select(
        "id",
        "order_channel_lower",
        F.max("row_seq_plus_prev_sqft").over(w6).alias("last3_max_row_seq_plus_prev_sqft"),
        F.avg("row_seq_plus_prev_sqft").over(w6).alias("last3_avg_row_seq_plus_prev_sqft"),
        F.min("row_seq_plus_prev_sqft").over(w6).alias("last3_min_row_seq_plus_prev_sqft"),
        F.lead("prev_sqft", 1).over(w7).alias("next_prev_sqft"),
    )
)
