from pyspark.sql import functions as F

joined = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.opened_date"), F.col("p.city"), F.col("p.store_name"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("quantity") >= 245, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("quantity_per_berlin"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
    )
    .orderBy("quantity_stddev")
)
