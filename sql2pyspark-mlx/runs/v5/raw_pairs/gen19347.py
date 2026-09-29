from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("order_channel").orderBy(F.col("event_seq").desc_nulls_last(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-18").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.ceil("sqft").alias("sqft_whole"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

metrics = (
    filtered
    .withColumn("prev_sqft", F.lag("sqft").over(w_recent))
    .withColumn("prev_sqft_whole", F.lag("sqft_whole").over(w_recent))
    .withColumn("next_sqft_whole", F.lead("sqft_whole").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent))
)

with_last = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"), F.col("c.shipping_fee"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel").alias("order_channel"))
)

deduped = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "prev_sqft_whole",
        "next_sqft_whole",
        "event_seq",
        "shipping_fee",
        "order_channel",
    )
)

result = (
    deduped
    .select(
        "id",
        "opened_date",
        "prev_sqft_whole",
        "next_sqft_whole",
        "event_seq",
        "shipping_fee",
    )
    .orderBy(F.col("shipping_fee").asc_nulls_first())
)
