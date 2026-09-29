from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy("revenue", "id")
w_part = Window.partitionBy("store_id")
w_hist = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .select(
        "id",
        "product_id",
        "revenue",
        F.lag("revenue", 1, 0).over(w_running).alias("prev_revenue"),
        F.lag("revenue", 2).over(w2).alias("prev_revenue_2"),
        F.stddev_samp("units").over(w_part).alias("store_id_stddev_samp_units"),
        F.last("units", ignorenulls=True).over(w_hist).alias("last_known_units"),
        F.count("units").over(w5).alias("product_id_count_units"),
    )
)
