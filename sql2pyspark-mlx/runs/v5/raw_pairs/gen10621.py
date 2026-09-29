from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("region").orderBy("city_length")
w4 = Window.partitionBy("city")

base = (
    df_stores
    .filter((F.col("sqft") < 7889) & (F.col("opened_date") > F.lit("2023-06-25").cast("date")))
    .select(
        "*",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.length("city").alias("city_length"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

kept_keys = (
    with_last
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("region") != "West")
        .select("city")
    )
)

kept_rows = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.channel") == "store"), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("id")
    .subtract(
        df_stores
        .filter(F.lower(F.trim("store_name")) == "downtown express")
        .select("id")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_anti")
)

scored = (
    kept
    .filter(F.col("region") == "West")
    .withColumn("region_min_city_length", F.min("city_length").over(w2))
    .withColumn("city_length_cume_dist", F.cume_dist().over(w3))
    .withColumn("city_avg_city_length", F.avg("city_length").over(w4))
)

result = (
    scored
    .groupBy(F.date_trunc("quarter", "opened_date_month_end").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
    )
)
