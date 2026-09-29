from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-05-10"))) & (F.col("quantity") == 148))
    .select("*", F.year("last_restock").alias("last_restock_year"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.last_restock",
        "e.last_restock_year",
        F.col("d.city"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select(
        "s.*",
        F.col("p2.store_name"),
        F.col("p2.opened_date"),
        F.col("p2.city").alias("store_city"),
    )
)

valid = (
    joined
    .filter((~F.col("city").eqNullSafe(F.lit("Tokyo"))) & F.col("last_restock").between("2025-06-01", "2025-07-27"))
    .withColumn("days_opened_date_to_last_restock", F.datediff("last_restock", "opened_date"))
    .withColumn("last_restock_year_tier", F.when(F.col("last_restock_year") >= 76, "High").when(F.col("last_restock_year") >= 34, "Medium").otherwise("Low"))
    .withColumn("store_city_filled", F.coalesce("store_city", F.lit("UNKNOWN")))
)

result = (
    valid
    .alias("e")
    .join(df_stores.alias("dim"), F.col("e.store_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["store_city", "inventory_city"], ["store_city"], []],
        F.col("dim.city").alias("store_city"),
        F.col("e.city").alias("inventory_city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("e.days_opened_date_to_last_restock")), 2).alias("avg_days_opened_date_to_last_restock"),
        F.grouping_id().alias("gid"),
    )
)
