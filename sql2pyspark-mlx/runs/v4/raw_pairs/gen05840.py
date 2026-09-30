from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-12-30").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 182, "High").when(F.col("quantity") >= 41, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.opened_date"), F.col("p.city"), F.col("p.sqft"))
)

filtered = (
    joined
    .filter((F.col("last_restock") >= F.lit("2024-02-22").cast("date")) & F.col("city").isNotNull())
    .select("*", F.dayofweek("opened_date").alias("opened_date_dayofweek"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("quantity") >= 235, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy(F.col("num_cities").desc_nulls_last())
)
