from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy("quantity", "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("city").orderBy("last_known_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-06-03").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.concat("store_name", F.lit(" "), "city").alias("store_name_city"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "city",
        "sqft",
        F.first("sqft", ignorenulls=True).over(w).alias("first_known_sqft"),
        F.lead("sqft", 2).over(w2).alias("next_sqft"),
        F.sum("sqft").over(w).alias("running_sqft"),
        F.last("sqft", ignorenulls=True).over(w).alias("last_known_sqft"),
        F.last("sqft", ignorenulls=True).over(w).alias("last_known_sqft_2"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.running_sqft",
        "s.last_known_sqft",
        F.col("c.last_restock"),
        F.col("c.quantity"),
    )
)

metrics_stores = (
    latest_match
    .select(
        "id",
        "last_known_sqft",
        "last_restock",
        "quantity",
        F.ntile(4).over(w4).alias("quantity_bucket"),
        F.round((F.col("running_sqft") / F.sum("running_sqft").over(w5)), 4).alias("running_sqft_share"),
        F.sum("last_known_sqft").over(w6).alias("cumulative_last_known_sqft"),
        F.min("running_sqft").over(w5).alias("city_min_running_sqft"),
    )
)

eligible = (
    metrics_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.customer_id").isin([21, 35])), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2])), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("running_sqft_share") >= 0.67, "LARGE").otherwise("SMALL").alias("running_sqft_share_tier"),
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("inventory_quantity"), 2).alias("avg_inventory_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count_2"),
    )
    .orderBy(F.col("running_sqft_share_tier").desc())
)
