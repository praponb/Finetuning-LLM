from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id").orderBy("revenue_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("units_filled", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("product_id").orderBy("units_filled", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("units").desc())
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("revenue").alias("min_store_sale_revenue"))

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-16").cast("date"))
    .select(
        "*",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

scored = (
    cleaned
    .withColumn("product_id_max_units", F.max("units").over(w))
    .withColumn("first_known_revenue_filled", F.first("revenue_filled", ignorenulls=True).over(w2))
    .withColumn("revenue_avg_3_prior", F.avg("revenue").over(w3))
    .withColumn("units_filled_bucket", F.ntile(4).over(w4))
)

cleaned_store_sales = (
    scored
    .filter(F.col("revenue_filled") < 32)
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_filled",
        "units_filled",
        "product_id_max_units",
        "first_known_revenue_filled",
        "units_filled_bucket",
        F.round((F.col("units") * 0.9), 2).alias("units_scaled"),
        (F.col("units_filled") - F.col("units")).alias("units_filled_minus_units"),
    )
)

base = (
    cleaned_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-10-12"))) & F.col("units").between(16, 34))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "units_filled",
        "product_id_max_units",
        "units_filled_bucket",
        "units_filled_minus_units",
        (F.col("units") - F.col("first_known_revenue_filled")).alias("units_minus_first_known_revenue_filled"),
        F.round((F.col("units") * 0.85), 2).alias("units_scaled_2"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

grouped = (
    latest
    .cube(
        "product_id",
        F.when(F.col("units_minus_first_known_revenue_filled") >= 84, "HIGH").otherwise("LOW").alias("units_minus_first_known_revenue_filled_tier"),
    )
    .agg(
        F.percentile_approx("units_scaled_2", 0.5).alias("units_scaled_2_p50"),
        F.avg("units_filled_bucket").alias("avg_units_filled_bucket"),
        F.percentile_approx("units_filled_minus_units", 0.95).alias("units_filled_minus_units_p95"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "units_minus_first_known_revenue_filled_tier",
        "units_scaled_2_p50",
        "avg_units_filled_bucket",
        "units_filled_minus_units_p95",
        "cnt",
        "row_count",
        F.col("min_store_sale_revenue"),
    )
)
