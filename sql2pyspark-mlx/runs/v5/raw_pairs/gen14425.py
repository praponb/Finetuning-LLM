from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date_plus_months")
w2 = Window.orderBy("sqft", "id")
w3 = Window.partitionBy("region", "city").orderBy("opened_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("opened_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "region").orderBy(F.col("opened_date_plus_months").desc(), "id")
w6 = Window.partitionBy("city", "region").orderBy("first_known_sqft", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(~F.col("region").eqNullSafe(F.lit("Central")))
    .select("*", F.add_months("opened_date", 12).alias("opened_date_plus_months"))
)

deduped = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .filter(F.col("region").isin(["South", "West", "North"]))
    .withColumn("next_sqft", F.lead("sqft", 1).over(w2))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w3))
    .withColumn("first_known_sqft_2", F.first("sqft", ignorenulls=True).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

deduped_stores = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "opened_date_plus_months",
        "next_sqft",
        "first_known_sqft",
        "event_seq",
    )
)

latest_match = (
    deduped_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"), F.col("c.channel").alias("order_channel"))
)

result = (
    latest_match
    .select(
        "id",
        "opened_date_plus_months",
        "next_sqft",
        "shipping_fee",
        "order_channel",
    )
    .orderBy(F.col("next_sqft").desc_nulls_first())
)
