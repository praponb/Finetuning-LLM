from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w3 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-2, -1)
w_running = Window.orderBy(F.col("units").desc(), "id")
w5 = Window.orderBy("prev_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("next_revenue").asc_nulls_first())
w_hist = Window.orderBy("prev_next_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_store_sales
    .select(
        "*",
        F.lag("units").over(w).alias("prev_units"),
        F.lead("units").over(w).alias("next_units"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.row_number().over(w).alias("event_seq"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "id",
        "units",
        "prev_units",
        "next_revenue",
        "event_seq",
        F.lead("next_units").over(w2).alias("next_next_units"),
        F.lag("next_units").over(w2).alias("prev_next_units"),
        F.lead("event_seq").over(w2).alias("next_event_seq"),
        F.lag("event_seq").over(w2).alias("prev_event_seq"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("prev_units").over(w3).alias("prev_units_avg_2_prior"),
    )
)

filtered = (
    with_windows_store_sales
    .filter(F.col("prev_event_seq") == 4)
    .withColumn("prev_units_plus_prev_units_avg_2_prior", (F.col("prev_units") + F.col("prev_units_avg_2_prior")))
)

enriched = (
    filtered
    .select(
        "*",
        F.ntile(4).over(w_running).alias("units_bucket"),
        F.sum("next_next_units").over(w5).alias("running_next_next_units"),
        F.cume_dist().over(w6).alias("next_revenue_cume_dist"),
    )
)

enriched_store_sales = (
    enriched
    .filter(F.col("prev_units") == 18)
    .select(
        "id",
        "prev_units",
        "next_revenue",
        "next_next_units",
        "prev_next_units",
        "next_event_seq",
        "row_seq",
        "prev_units_plus_prev_units_avg_2_prior",
        "units_bucket",
        "running_next_next_units",
        "next_revenue_cume_dist",
        F.last("next_next_units", ignorenulls=True).over(w_hist).alias("last_known_next_next_units"),
        F.first("event_seq", ignorenulls=True).over(w8).alias("first_known_event_seq"),
        F.last("prev_next_units", ignorenulls=True).over(w_hist).alias("last_known_prev_next_units"),
    )
)

result = (
    enriched_store_sales
    .select(
        "id",
        "next_revenue",
        "next_next_units",
        "prev_next_units",
        "next_event_seq",
        "row_seq",
        "prev_units_plus_prev_units_avg_2_prior",
        "running_next_next_units",
        "last_known_next_next_units",
    )
    .orderBy("next_revenue")
)
