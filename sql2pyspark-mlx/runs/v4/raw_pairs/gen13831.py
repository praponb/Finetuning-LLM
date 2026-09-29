from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("sqft")
w4 = Window.partitionBy("city")

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-11-29").cast("date")) & F.col("region").isNotNull() & (F.col("city") == "Bangkok"))
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    latest
    .withColumn("sqft_avg_2", F.avg("sqft").over(w2))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w3))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < "2025-02-01"), "left_anti")
)

result = (
    not_flagged
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date_plus_months"))
    .groupingSets(
        [["activity_month", "city"], ["activity_month"], []],
        "activity_month",
        "city",
    )
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count("*").alias("event_count"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("sqft_per_singapore"),
    )
    .orderBy(F.col("num_cities").desc())
)
