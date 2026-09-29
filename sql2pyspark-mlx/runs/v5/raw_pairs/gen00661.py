from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sqft", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-08-30"))) & F.col("region").isNotNull())
    .withColumn("region_lower", F.lower("region"))
)

metrics = (
    valid
    .filter(F.upper(F.trim("store_name")) == "AIRPORT KIOSK")
    .select(
        "*",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.avg("sqft").over(w3).alias("sqft_avg_3_prior"),
        F.avg("sqft").over(w4).alias("sqft_avg_6_prior"),
    )
)

base = (
    metrics
    .filter((~F.col("region").eqNullSafe(F.lit("South"))) & (F.col("opened_date") > F.lit("2023-04-11").cast("date")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "region_lower",
        "running_sqft",
        "sqft_avg_3_prior",
        "sqft_avg_6_prior",
        F.trim("store_name").alias("clean_store_name"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isNull()), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

cleaned = (
    with_last
    .filter(F.col("sqft_avg_3_prior").between(6083, 12827))
    .withColumn("sqft_avg_3_prior_scaled", F.round((F.col("sqft_avg_3_prior") * 0.9), 2))
    .withColumn("resolved_region", F.coalesce("region", F.lit("N/A")))
)

result = (
    cleaned
    .groupBy("region", "city")
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("quantity"))).alias("west_total_quantity"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft_avg_3_prior")).otherwise(0)).alias("singapore_sqft_avg_3_prior"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
    )
    .orderBy(F.col("bangkok_sqft").asc_nulls_last())
)
