from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_recent = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.opened_date")), "left_anti")
)

scored = (
    not_flagged
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.sum("sqft").over(w_recent).alias("running_sqft"),
    )
)

not_flagged_stores = (
    scored
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_semi")
)

latest_match = (
    not_flagged_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

cleaned = (
    latest_match
    .filter(F.col("region").isNotNull() & (F.upper(F.trim("store_name")) == "AIRPORT KIOSK"))
    .select(
        "id",
        "city",
        "sqft",
        "running_sqft",
        F.round((F.col("running_sqft") * 0.85), 2).alias("running_sqft_scaled"),
    )
)

grouped = (
    cleaned
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("running_sqft")).otherwise(0)).alias("singapore_running_sqft"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.round(F.avg("running_sqft"), 2).alias("avg_running_sqft"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.avg(F.when(F.col("city") == "Singapore", F.col("running_sqft_scaled"))).alias("singapore_avg_running_sqft_scaled"),
    )
    .filter(F.col("singapore_avg_running_sqft_scaled") >= 0)
)

result = (
    grouped
    .select(
        "city",
        "singapore_running_sqft",
        "berlin_events",
        "avg_running_sqft",
        "singapore_avg_running_sqft_scaled",
    )
    .orderBy(F.col("berlin_events").desc())
)
