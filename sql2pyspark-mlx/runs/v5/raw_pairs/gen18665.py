from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w_recent = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region", "city").orderBy("opened_date", "id")

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-20").cast("date")) & (F.col("region").eqNullSafe(F.lit("Central"))))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.regexp_replace("store_name", " ", "").alias("store_name_slug"),
    )
)

scored = (
    valid
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "store_name_slug",
        F.last("sqft", ignorenulls=True).over(w).alias("last_known_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.first("sqft", ignorenulls=True).over(w_recent).alias("first_known_sqft"),
    )
)

scored_stores = (
    scored
    .withColumn("next_sqft_share", F.lead("sqft_share").over(w4))
    .withColumn("next_last_known_sqft", F.lead("last_known_sqft").over(w4))
    .withColumn("event_seq", F.row_number().over(w4))
)

not_flagged = (
    scored_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.discount").between(0.14, 0.39)), "left_anti")
)

valid_stores = (
    not_flagged
    .filter(F.col("region").isNotNull() & (F.col("opened_date") > F.lit("2023-08-07").cast("date")))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "store_name_slug",
        "last_known_sqft",
        "sqft_share",
        "first_known_sqft",
        "next_sqft_share",
        "event_seq",
        F.round((F.col("event_seq") / F.col("sqft")), 2).alias("event_seq_per_sqft"),
    )
)

result = (
    valid_stores
    .select(
        "id",
        "region",
        "store_name_slug",
        "last_known_sqft",
        "first_known_sqft",
        "next_sqft_share",
        "event_seq",
        "event_seq_per_sqft",
    )
    .orderBy("first_known_sqft")
)
