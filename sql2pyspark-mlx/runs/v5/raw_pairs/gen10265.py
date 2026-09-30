from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("region", "city").orderBy("event_seq", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region").eqNullSafe(F.lit("North"))))
    .select("*", F.add_months("opened_date", 3).alias("opened_date_plus_months"))
)

metrics = (
    cleaned
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.first("sqft", ignorenulls=True).over(w).alias("first_known_sqft"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq"),
        F.last("sqft", ignorenulls=True).over(w4).alias("last_known_sqft"),
        F.last("region", ignorenulls=True).over(w5).alias("last_known_region"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "region",
        "sqft",
        "first_known_sqft",
        "event_seq",
        "last_known_region",
    )
)

latest_match = (
    latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.first_known_sqft",
        "s.event_seq",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

matched_keys = (
    latest_match
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["West", "East"]))
        .select("id", "region")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 2, 5])), "left_semi")
)

grouped = (
    eligible
    .groupBy(F.date_trunc("week", "inventory_last_restock").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("first_known_sqft", 0.5).alias("first_known_sqft_p50"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.sum(F.when(F.col("region") == "Central", F.col("first_known_sqft")).otherwise(0)).alias("central_first_known_sqft"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .filter(F.col("first_known_sqft_p50") > 0)
)

result = (
    grouped
    .select("week_start", "region_list", "quantity_p90", "central_first_known_sqft")
)
