from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("product_id").orderBy("revenue", "id")
w3 = Window.partitionBy("store_id").orderBy("units", "id")
w4 = Window.partitionBy("city").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "store_id").orderBy(F.col("sqft").asc_nulls_last())
w6 = Window.orderBy(F.col("units").desc(), "id")
w7 = Window.partitionBy("city", "product_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("revenue_avg_6_prior", "id")
w9 = Window.partitionBy("product_id").orderBy("prev_revenue_bucket", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("product_id").orderBy("sqft_percent_rank", "id").rowsBetween(-5, -1)
w11 = Window.partitionBy("product_id").orderBy("last_known_revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("prev_revenue_bucket", "id").rowsBetween(-5, -1)
w13 = Window.orderBy("revenue_avg_6_prior", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-02-16").cast("date"))
    .withColumn("revenue_per_unit", (F.col("revenue") * F.col("units")))
)

scored = (
    valid
    .withColumn("revenue_avg_6_prior", F.avg("revenue").over(w))
    .withColumn("revenue_bucket", F.ntile(10).over(w2))
)

enriched = (
    scored
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.sqft"), F.col("d.store_name"), F.col("d.city"))
)

scored_store_sales = (
    enriched
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_avg_6_prior",
        "revenue_bucket",
        "sqft",
        F.lag("revenue_bucket", 1).over(w3).alias("prev_revenue_bucket"),
        F.first("sqft", ignorenulls=True).over(w4).alias("first_known_sqft"),
        F.percent_rank().over(w5).alias("sqft_percent_rank"),
        F.row_number().over(w6).alias("event_seq"),
        F.last("revenue_per_unit", ignorenulls=True).over(w7).alias("last_known_revenue_per_unit"),
    )
)

valid_store_sales = (
    scored_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-07-17").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_avg_6_prior",
        "revenue_bucket",
        "sqft",
        "prev_revenue_bucket",
        "first_known_sqft",
        "sqft_percent_rank",
        "event_seq",
        "last_known_revenue_per_unit",
        (F.col("sqft") / F.nullif("sqft_percent_rank", F.lit(0))).alias("sqft_per_sqft_percent_rank"),
    )
)

with_windows = (
    valid_store_sales
    .select(
        "id",
        "sale_date",
        "units",
        "revenue_avg_6_prior",
        "prev_revenue_bucket",
        "first_known_sqft",
        "sqft_percent_rank",
        "sqft_per_sqft_percent_rank",
        F.lead("prev_revenue_bucket", 2).over(w8).alias("next_prev_revenue_bucket"),
        F.avg("last_known_revenue_per_unit").over(w9).alias("last_known_revenue_per_unit_avg_5"),
        F.avg("revenue").over(w10).alias("revenue_avg_5_prior"),
        F.sum("sqft").over(w11).alias("cumulative_sqft"),
    )
)

enriched_store_sales = (
    with_windows
    .filter(F.col("next_prev_revenue_bucket").isNotNull())
    .select(
        "id",
        "sale_date",
        "units",
        "prev_revenue_bucket",
        "first_known_sqft",
        "next_prev_revenue_bucket",
        "revenue_avg_5_prior",
        "cumulative_sqft",
        F.avg("sqft_per_sqft_percent_rank").over(w12).alias("sqft_per_sqft_percent_rank_avg_5_prior"),
        F.row_number().over(w13).alias("seq_no"),
    )
)

result = (
    enriched_store_sales
    .select("id", "sale_date", "units", "prev_revenue_bucket", "cumulative_sqft")
)
