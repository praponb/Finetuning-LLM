from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units_whole", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("units_whole")
w3 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy(F.col("units_whole_avg_2").desc(), "id")

valid = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-02-17").cast("date")) & (F.col("units") >= 22))
    .select(
        "*",
        F.least("units", "revenue").alias("least_units_revenue"),
        F.when(F.col("units") >= 36, "High").when(F.col("units") >= 8, "Medium").otherwise("Low").alias("units_tier"),
    )
)

base = (
    valid
    .filter(F.col("sale_date") >= F.lit("2023-05-09").cast("date"))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "units_tier",
        F.floor("units").alias("units_whole"),
    )
)

scored = (
    base
    .select(
        "*",
        F.avg("units").over(w).alias("units_avg_6"),
        F.cume_dist().over(w2).alias("units_whole_cume_dist"),
        F.avg("units_whole").over(w3).alias("units_whole_avg_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("units_whole").between(24, 35))
    .select(
        "*",
        F.round((F.col("units_whole_cume_dist") * 1.07), 2).alias("units_whole_cume_dist_scaled"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "units",
        "units_tier",
        "units_whole_cume_dist",
        "units_whole_avg_2",
        "units_whole_cume_dist_scaled",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("units_whole_avg_2") >= 78, "PREMIUM").otherwise("STANDARD").alias("units_whole_avg_2_tier"),
        F.when(F.col("units") >= 32, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("units_whole_avg_2"), 2).alias("avg_units_whole_avg_2"),
        F.percentile_approx("units_whole_avg_2", [0.25, 0.5, 0.75]).alias("units_whole_avg_2_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.bit_or("units").alias("units_bit_or"),
    )
    .filter(F.col("distinct_ids_2") > 1)
    .orderBy(F.col("units_whole_avg_2_tier").asc_nulls_last())
)
