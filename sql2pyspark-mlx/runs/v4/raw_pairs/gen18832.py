from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())
w_grp = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-2, -1)

base = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-09-13").cast("date"))
    .select(
        "*",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

filtered = (
    base
    .filter(F.col("opened_date") >= F.lit("2023-12-09").cast("date"))
    .select(
        "*",
        F.lower("store_name").alias("store_name_lower"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
        F.sum("sqft_filled").over(w_grp).alias("running_sqft_filled"),
        F.avg("sqft_filled").over(w3).alias("sqft_filled_avg_2_prior"),
    )
)

result = (
    scored
    .cube(F.when(F.col("sqft") >= 17152, "LARGE").otherwise("SMALL").alias("sqft_tier"))
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
)
