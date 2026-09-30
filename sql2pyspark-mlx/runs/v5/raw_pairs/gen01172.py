from pyspark.sql import functions as F

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        "region",
        "city",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft")).otherwise(0)).alias("berlin_sqft"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft"))).alias("east_total_sqft"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
    )
    .filter(F.col("east_total_sqft") > 0)
    .orderBy("east_total_sqft")
)
