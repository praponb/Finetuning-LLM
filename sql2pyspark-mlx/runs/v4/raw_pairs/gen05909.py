from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first())
w3 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")
w4 = Window.partitionBy("product_id", "store_id")
w5 = Window.partitionBy("store_id", "product_id")
w6 = Window.partitionBy("product_id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-12-21").cast("date")) & F.col("product_id").isin([1, 6, 9]))
    .select(
        "*",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
        F.round((F.col("units") * 100), 2).alias("units_adjusted"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.sum("units").over(w).alias("running_units"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
        F.lag("units_adjusted", 2).over(w3).alias("prev_units_adjusted"),
        F.round((F.col("revenue") / F.sum("revenue").over(w4)), 4).alias("revenue_share"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "*",
        F.corr("revenue", "units").over(w5).alias("corr_revenue_units"),
        F.avg("prev_units_adjusted").over(w6).alias("product_id_avg_prev_units_adjusted"),
    )
)

daily = (
    with_windows_store_sales
    .groupBy(
        "product_id",
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        "store_id",
    )
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("units_bit_and") < 1)
)

result = (
    daily
    .select("product_id", "quarter_start", "store_id", "units_bit_and", "event_count")
    .orderBy(F.col("event_count").desc())
)
