from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-03-25")))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

with_windows = (
    cleaned
    .withColumn("units_bucket", F.ntile(10).over(w_part))
    .withColumn("first_known_units", F.first("units", ignorenulls=True).over(w2))
)

valid = (
    with_windows
    .filter(F.col("sale_date") > F.lit("2023-09-23").cast("date"))
    .withColumn("revenue_whole", F.ceil("revenue"))
    .withColumn("first_known_units_per_units_bucket", F.round((F.col("first_known_units") / F.col("units_bucket")), 2))
)

enriched = (
    valid
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.sale_date",
        "e.revenue",
        "e.sale_date_month_end",
        "e.units_bucket",
        "e.revenue_whole",
        F.col("s.city"),
        F.col("s.store_name"),
    )
)

result = (
    enriched
    .withColumn("activity_month", F.date_trunc("MONTH", "sale_date_month_end"))
    .withColumn("revenue_whole_tier", F.when(F.col("revenue_whole") >= 71, "HIGH").otherwise("LOW"))
    .cube("activity_month", "city", "revenue_whole_tier")
    .agg(
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.percentile_approx("revenue_whole", [0.25, 0.5, 0.75]).alias("revenue_whole_quartiles"),
        F.percentile_approx("revenue_whole", [0.25, 0.5, 0.75]).alias("revenue_whole_quartiles_2"),
        F.percentile_approx("units_bucket", [0.25, 0.5, 0.75]).alias("units_bucket_quartiles"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_id"),
    )
)
