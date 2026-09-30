from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("units", "id")
w3 = Window.orderBy("latest_rank", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("latest_rank_minus_revenue").desc_nulls_last())
w6 = Window.orderBy("revenue", "id")
w_part = Window.orderBy(F.col("first_known_latest_rank_minus_revenue").desc(), "id")
w8 = Window.orderBy(F.col("event_seq").asc_nulls_last())

base = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .select("*", F.add_months("sale_date", 6).alias("sale_date_plus_months"))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "sale_date", "units", "revenue")
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("latest_rank") >= 1)
    .select(
        "id",
        "revenue",
        "latest_rank",
        F.when(F.col("units") >= 39, "High").when(F.col("units") >= 17, "Medium").otherwise("Low").alias("units_band"),
    )
)

filtered_store_sales = (
    filtered
    .filter(F.col("revenue").between(2200, 2300))
    .select(
        "*",
        (F.col("latest_rank") - F.col("revenue")).alias("latest_rank_minus_revenue"),
    )
)

scored = (
    filtered_store_sales
    .select(
        "id",
        "revenue",
        "units_band",
        "latest_rank_minus_revenue",
        F.avg("revenue").over(w3).alias("revenue_avg_3"),
        F.first("latest_rank_minus_revenue", ignorenulls=True).over(w4).alias("first_known_latest_rank_minus_revenue"),
        F.dense_rank().over(w5).alias("latest_rank_minus_revenue_dense_rank"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "revenue_avg_3",
        "first_known_latest_rank_minus_revenue",
        F.row_number().over(w_part).alias("event_seq_2"),
        F.dense_rank().over(w8).alias("event_seq_dense_rank"),
    )
)

result = (
    with_windows
    .select("id", "revenue_avg_3", "event_seq_dense_rank")
    .orderBy("event_seq_dense_rank")
)
