from pyspark.sql import functions as F

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
    )
)
