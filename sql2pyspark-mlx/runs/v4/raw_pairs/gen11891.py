from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("revenue", "id")
w4 = Window.orderBy(F.col("units").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

with_windows = (
    df_store_sales
    .select(
        "*",
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.sum("units").over(w2).alias("running_units"),
        F.lag("units", 2).over(w3).alias("prev_units"),
        F.ntile(10).over(w4).alias("units_bucket"),
        F.ntile(5).over(w5).alias("units_bucket_2"),
    )
)

result = (
    with_windows
    .groupBy(
        "store_id",
        F.when(F.col("units_share") >= 0.86, "PREMIUM").otherwise("STANDARD").alias("units_share_tier"),
    )
    .agg(
        F.percentile_approx("prev_units", 0.5).alias("prev_units_p50"),
        F.count("*").alias("row_count"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
)
