from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w_running = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-04-01").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

cleaned = (
    valid
    .filter(F.col("sale_date") > F.lit("2023-05-05").cast("date"))
    .select("*", F.last_day("sale_date").alias("sale_date_month_end"))
)

with_windows = (
    cleaned
    .filter(F.col("revenue").between(2500, 3600))
    .select(
        "id",
        "sale_date",
        "revenue",
        "revenue_int",
        "sale_date_month_end",
        F.lag("revenue", 2).over(w).alias("prev_revenue"),
        F.ntile(10).over(w_running).alias("revenue_bucket"),
    )
)

cleaned_store_sales = (
    with_windows
    .filter(F.col("sale_date") <= "2024-07-01")
    .select(
        "id",
        "revenue",
        "revenue_int",
        "prev_revenue",
        "revenue_bucket",
        F.date_trunc("WEEK", "sale_date_month_end").alias("sale_date_month_end_period"),
        F.greatest("revenue_int", "revenue").alias("greatest_revenue_int_revenue"),
    )
)

result = (
    cleaned_store_sales
    .groupBy(
        F.when(F.col("prev_revenue") >= 4100, "PREMIUM").otherwise("STANDARD").alias("prev_revenue_tier"),
        F.date_trunc("QUARTER", "sale_date_month_end_period").alias("quarter_start"),
    )
    .agg(
        F.min("sale_date_month_end_period").alias("first_sale_date_month_end_period"),
        F.max("sale_date_month_end_period").alias("last_sale_date_month_end_period"),
    )
)
