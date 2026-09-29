from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-04-17").cast("date")) & F.col("store_id").isin([1, 4, 5, 6]))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

cleaned_inventory = (
    cleaned
    .filter(F.col("last_restock") < "2024-12-01")
    .withColumn("quantity_rounded_whole", F.ceil("quantity_rounded"))
)

cleaned_inventory_2 = (
    cleaned_inventory
    .filter(F.col("last_restock") >= F.lit("2024-04-19").cast("date"))
    .withColumn("quantity_rounded_whole_whole", F.floor("quantity_rounded_whole"))
)

enriched = (
    cleaned_inventory_2
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.sqft"), F.col("p2.city"), F.col("p2.opened_date"))
)

matched_keys = (
    enriched
    .select("id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 3, 4]))
        .select("id", "store_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("activity_month", F.date_trunc("month", "opened_date"))
    .groupingSets(
        [["store_id", "activity_month"], ["store_id"]],
        "store_id",
        "activity_month",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft"))).alias("singapore_total_sqft"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
    )
)
