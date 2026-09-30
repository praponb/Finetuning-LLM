from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w2 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")

with_windows = (
    df_store_sales
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("first_known_units", F.first("units", ignorenulls=True).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

filtered = (
    with_windows
    .filter(F.col("sale_date") >= F.lit("2023-11-29").cast("date"))
    .select("*", (F.col("revenue") * F.col("seq_no")).alias("revenue_times_seq_no"))
)

base = (
    filtered
    .filter(F.col("sale_date") >= F.lit("2024-01-11").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "seq_no",
        "first_known_units",
        "revenue_times_seq_no",
        (F.col("seq_no") - F.col("revenue_times_seq_no")).alias("seq_no_minus_revenue_times_seq_no"),
    )
)

result = (
    base
    .select("id", "store_id", "product_id", "sale_date", "revenue", "first_known_units")
    .orderBy("sale_date")
)
