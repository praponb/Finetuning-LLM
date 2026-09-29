from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("least_units_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy("units", "id")
w4 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w5 = Window.orderBy("units", "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("units_bucket").asc_nulls_last(), "id")

valid = (
    df_store_sales
    .filter(F.col("units") == 34)
    .withColumn("least_units_revenue", F.least("units", "revenue"))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
)

enriched = (
    valid
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "least_units_revenue",
        F.sum("revenue").over(w).alias("running_revenue"),
        F.sum("least_units_revenue").over(w2).alias("running_least_units_revenue"),
        F.ntile(3).over(w3).alias("units_bucket"),
        F.row_number().over(w4).alias("row_seq"),
        F.ntile(5).over(w5).alias("units_bucket_2"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

enriched_store_sales = (
    latest
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.least_units_revenue",
        "src.units_bucket",
        "src.row_seq",
        "src.latest_rank",
        F.col("p.product_name"),
    )
)

result = (
    enriched_store_sales
    .withColumn("least_units_revenue_tier", F.when(F.col("least_units_revenue") >= 71, "LARGE").otherwise("SMALL"))
    .withColumn("units_bucket_tier", F.when(F.col("units_bucket") >= 8, "PREMIUM").otherwise("STANDARD"))
    .cube("product_id", "least_units_revenue_tier", "units_bucket_tier")
    .agg(
        F.percentile_approx("units_bucket", 0.9).alias("units_bucket_p90"),
        F.percentile_approx("least_units_revenue", 0.5).alias("least_units_revenue_p50"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("least_units_revenue_p50").desc())
)
