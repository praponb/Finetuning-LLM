from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("region", "city").orderBy("opened_date_month_end", "id")
w_hist = Window.partitionBy("region").orderBy("opened_date_month_end", "id")
w3 = Window.orderBy("row_seq", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("city").orderBy("event_seq", "id")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-03-30").cast("date")) & (F.col("city") == "Singapore"))
    .withColumn("region_lower", F.lower("region"))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("region_clean", F.coalesce("region", F.lit("OTHER")))
)

with_windows = (
    valid
    .filter(F.col("region") != "Central")
    .withColumn("prev_sqft", F.lag("sqft").over(w_running))
    .withColumn("next_sqft", F.lead("sqft").over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("event_seq", F.row_number().over(w_hist))
)

kept = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 2, 3, 4])), "left_semi")
)

enriched = (
    kept
    .withColumn("next_sqft_avg_3_prior", F.avg("next_sqft").over(w3))
    .withColumn("prev_prev_sqft", F.lag("prev_sqft", 2, 0).over(w4))
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "region_lower",
        "opened_date_month_end",
        "region_clean",
        "next_sqft",
        "prev_prev_sqft",
    )
    .orderBy("prev_prev_sqft")
)
