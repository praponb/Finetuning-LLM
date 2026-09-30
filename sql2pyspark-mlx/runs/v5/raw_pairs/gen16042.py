from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.min("revenue").over(w_grp).alias("running_min_revenue"),
        F.sum("revenue").over(w_grp).alias("running_sum_revenue"),
        F.avg("revenue").over(w_grp).alias("running_avg_revenue"),
        F.count("revenue").over(w_grp).alias("running_count_revenue"),
        F.lag("revenue", 1).over(w2).alias("prev_revenue"),
    )
)
