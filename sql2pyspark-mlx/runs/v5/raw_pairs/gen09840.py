from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy(F.col("least_revenue_whole_plus_units_rounded_revenue").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("revenue_whole")
w4 = Window.partitionBy("product_id").orderBy(F.col("least_revenue_whole_plus_units_rounded_revenue").desc_nulls_last())

filtered = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 4, 7]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.ceil("revenue").alias("revenue_whole"),
        F.round("units", 0).alias("units_rounded"),
    )
)

filtered_store_sales = (
    filtered
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-06-17")))
    .withColumn("revenue_whole_num", F.col("revenue_whole").cast("string"))
)

base = (
    filtered_store_sales
    .filter(F.col("store_id") == 4)
    .select(
        "*",
        (F.col("revenue_whole") + F.col("units_rounded")).alias("revenue_whole_plus_units_rounded"),
    )
)

latest = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "units_rounded",
        "revenue_whole_plus_units_rounded",
    )
)

base_store_sales = (
    latest
    .filter(F.col("units") == 18)
    .select(
        "id",
        "product_id",
        "revenue",
        "revenue_whole_plus_units_rounded",
        F.ceil("revenue").alias("revenue_whole"),
    )
)

valid = (
    base_store_sales
    .filter(F.col("revenue") < 3000)
    .select(
        "id",
        "product_id",
        "revenue_whole",
        (F.col("revenue") - F.col("revenue_whole_plus_units_rounded")).alias("revenue_minus_revenue_whole_plus_units_rounded"),
        F.least("revenue_whole_plus_units_rounded", "revenue").alias("least_revenue_whole_plus_units_rounded_revenue"),
    )
)

enriched = (
    valid
    .filter(F.col("least_revenue_whole_plus_units_rounded_revenue") <= 67)
    .select(
        "id",
        "least_revenue_whole_plus_units_rounded_revenue",
        F.ntile(10).over(w2).alias("least_revenue_whole_plus_units_rounded_revenue_bucket"),
        F.rank().over(w3).alias("revenue_whole_rank"),
        F.rank().over(w4).alias("least_revenue_whole_plus_units_rounded_revenue_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("least_revenue_whole_plus_units_rounded_revenue") >= 69, "PREMIUM").otherwise("STANDARD").alias("least_revenue_whole_plus_units_rounded_revenue_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("least_revenue_whole_plus_units_rounded_revenue_bucket", [0.25, 0.5, 0.75]).alias("least_revenue_whole_plus_units_rounded_revenue_bucket_quartiles"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(
        F.col("least_revenue_whole_plus_units_rounded_revenue_tier").asc_nulls_first(),
    )
)
