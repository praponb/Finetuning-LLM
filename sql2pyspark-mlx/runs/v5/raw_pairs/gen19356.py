from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc_nulls_first())

enriched = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.min("units").over(w).alias("running_min_units"),
        F.avg("units").over(w).alias("running_avg_units"),
        F.sum("units").over(w).alias("running_sum_units"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    deduped
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.sqft"), F.col("p.city"))
)

cleaned = (
    joined
    .filter(F.col("running_min_units").isNotNull())
    .select(
        "*",
        (F.col("running_avg_units") * F.col("running_sum_units")).alias("running_avg_units_times_running_sum_units"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("running_avg_units") >= 42, "LARGE").otherwise("SMALL").alias("running_avg_units_tier"),
    )
    .agg(
        F.min("running_min_units").alias("min_running_min_units"),
        F.percentile_approx("running_avg_units_times_running_sum_units", [0.25, 0.5, 0.75]).alias("running_avg_units_times_running_sum_units_quartiles"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("units")).otherwise(0)).alias("berlin_units"),
    )
    .filter(F.col("berlin_rows") > 1)
)
