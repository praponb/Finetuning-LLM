from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("sale_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("running_stock_qty")

joined = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.stock_qty"))
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base = (
    latest
    .filter((F.col("sale_date") > F.lit("2023-06-18").cast("date")) & F.col("units").between(14, 22))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "stock_qty",
        (F.col("rn") - F.col("stock_qty")).alias("rn_minus_stock_qty"),
        F.month("sale_date").alias("sale_date_month"),
    )
)

with_windows = (
    base
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w2))
    .withColumn("cumulative_sale_date_month", F.sum("sale_date_month").over(w3))
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w4))
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "stock_qty",
        "rn_minus_stock_qty",
        "sale_date_month",
        "cumulative_stock_qty",
    )
)

result = (
    deduped
    .groupBy(
        F.when(F.col("sale_date_month") >= 86, "PREMIUM").otherwise("STANDARD").alias("sale_date_month_tier"),
    )
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.bit_or("units").alias("units_bit_or"),
    )
)
