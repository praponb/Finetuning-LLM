from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue_per_unit").desc())
w2 = Window.partitionBy("store_id").orderBy("units", "id")
w3 = Window.orderBy("revenue_per_unit", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city", "store_id").orderBy("store_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("city", "store_id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2024-04-19").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

enriched = (
    cleaned
    .withColumn("revenue_per_unit_cume_dist", F.cume_dist().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.region"), F.col("s.sqft"), F.col("s.opened_date"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.avg("units").over(w3).alias("units_avg_3"),
        F.sum("revenue_per_unit").over(w4).alias("running_revenue_per_unit"),
        F.sum("revenue_per_unit").over(w5).alias("cumulative_revenue_per_unit"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.city"), F.col("d.opened_date").alias("store_opened_date"))
)

with_windows_store_sales = (
    joined
    .select(
        "*",
        F.last("revenue_per_unit_cume_dist", ignorenulls=True).over(w6).alias("last_known_revenue_per_unit_cume_dist"),
        F.sum("units").over(w7).alias("cumulative_units"),
        F.sum("sqft").over(w8).alias("cumulative_sqft"),
        F.round((F.col("units") / F.sum("units").over(w9)), 4).alias("units_share"),
    )
)

result = (
    with_windows_store_sales
    .groupBy(
        F.when(F.col("revenue_per_unit_cume_dist") >= 0.71, "PREMIUM").otherwise("STANDARD").alias("revenue_per_unit_cume_dist_tier"),
    )
    .agg(
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(F.sum("units_avg_3") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("units_avg_3_per_new_york"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .orderBy(F.col("units_avg_3_per_new_york").desc_nulls_last())
)
