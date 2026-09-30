from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc(), "id")
w4 = Window.partitionBy("product_id", "store_id")

with_windows = (
    df_store_sales
    .select(
        "*",
        F.corr("revenue", "units").over(w).alias("corr_revenue_units"),
        F.sum("revenue").over(w2).alias("running_revenue"),
        F.ntile(5).over(w3).alias("revenue_bucket"),
        F.avg("units").over(w4).alias("product_id_avg_units"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        "corr_revenue_units",
        "running_revenue",
        "revenue_bucket",
        "product_id_avg_units",
    )
    .orderBy(F.col("running_revenue").desc_nulls_last(), "corr_revenue_units")
)
