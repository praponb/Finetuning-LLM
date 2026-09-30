from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("revenue", "id")
w4 = Window.partitionBy("store_id").orderBy("running_sum_revenue", "id")

latest = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-07-31")))
    .select(
        "id",
        "store_id",
        "revenue",
        "row_num",
        (F.col("row_num") - F.col("units")).alias("row_num_minus_units"),
    )
)

cleaned_store_sales = (
    cleaned
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .select(
        "*",
        (F.col("row_num_minus_units") - F.col("row_num")).alias("row_num_minus_units_minus_row_num"),
    )
)

enriched = (
    cleaned_store_sales
    .select(
        "id",
        "store_id",
        "row_num_minus_units",
        F.min("revenue").over(w2).alias("running_min_revenue"),
        F.avg("revenue").over(w2).alias("running_avg_revenue"),
        F.sum("revenue").over(w2).alias("running_sum_revenue"),
        F.count("revenue").over(w2).alias("running_count_revenue"),
        F.ntile(5).over(w3).alias("revenue_bucket"),
    )
)

enriched_store_sales = (
    enriched
    .select(
        "id",
        "running_sum_revenue",
        "running_count_revenue",
        "revenue_bucket",
        F.lead("row_num_minus_units").over(w4).alias("next_row_num_minus_units"),
        F.lag("row_num_minus_units").over(w4).alias("prev_row_num_minus_units"),
        F.lead("running_min_revenue").over(w4).alias("next_running_min_revenue"),
        F.lag("running_min_revenue").over(w4).alias("prev_running_min_revenue"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

result = (
    enriched_store_sales
    .groupBy(
        F.when(F.col("running_sum_revenue") >= 16500, "LARGE").otherwise("SMALL").alias("running_sum_revenue_tier"),
        F.when(F.col("next_running_min_revenue") >= 3900, "LARGE").otherwise("SMALL").alias("next_running_min_revenue_tier"),
    )
    .agg(
        F.round(F.avg("prev_running_min_revenue"), 2).alias("avg_prev_running_min_revenue"),
        F.min("prev_running_min_revenue").alias("min_prev_running_min_revenue"),
        F.round(F.avg("running_count_revenue"), 2).alias("avg_running_count_revenue"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("min_prev_running_min_revenue") >= 0)
)
