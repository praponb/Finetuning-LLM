from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.sum("revenue").over(w).alias("running_sum_revenue"),
        F.min("revenue").over(w).alias("running_min_revenue"),
        F.avg("revenue").over(w).alias("running_avg_revenue"),
    )
)
