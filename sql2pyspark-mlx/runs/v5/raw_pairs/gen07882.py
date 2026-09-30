from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")
w_running = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id")
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("rank_in_group").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("prev_revenue_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("product_id").orderBy(F.col("row_seq").desc(), "id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-03-22").cast("date")) & (F.col("sale_date") <= "2024-10-01"))
    .select(
        "*",
        F.ceil("revenue").alias("revenue_whole"),
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "revenue_whole",
        "rank_in_group",
        F.lead("revenue_whole").over(w_running).alias("next_revenue_whole"),
        F.lag("revenue_whole").over(w_running).alias("prev_revenue_whole"),
        F.lag("units").over(w_running).alias("prev_units"),
        F.row_number().over(w_running).alias("row_seq"),
        F.corr("revenue", "units").over(w3).alias("corr_revenue_units"),
    )
)

with_windows_store_sales = (
    with_windows
    .filter(F.col("sale_date").between("2023-03-01", "2024-04-20"))
    .select(
        "id",
        "sale_date",
        "revenue",
        "revenue_whole",
        "rank_in_group",
        "prev_units",
        "row_seq",
        F.row_number().over(w4).alias("event_seq"),
        F.last("row_seq", ignorenulls=True).over(w5).alias("last_known_row_seq"),
        F.row_number().over(w_part).alias("row_seq_2"),
    )
)

result = (
    with_windows_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        "prev_units",
        "row_seq",
        "event_seq",
        "last_known_row_seq",
    )
    .orderBy(F.col("revenue").desc())
)
