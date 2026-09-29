from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("revenue") < 1600)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.round((F.col("units") * 1.15), 2).alias("adjusted_units"),
    )
)

result = (
    cleaned
    .filter(F.col("adjusted_units") < 70)
    .select(
        "id",
        "product_id",
        "revenue",
        "adjusted_units",
        F.avg("revenue").over(w).alias("running_avg_revenue"),
        F.max("revenue").over(w).alias("running_max_revenue"),
        F.min("revenue").over(w).alias("running_min_revenue"),
        F.sum("units").over(w2).alias("running_units"),
    )
)
