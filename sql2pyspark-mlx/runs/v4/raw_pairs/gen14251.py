from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("opened_date", "id")
w_part = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("opened_date", "id")
w_part_2 = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "*",
        F.lead("sqft", 1).over(w_running).alias("next_sqft"),
        F.avg("sqft").over(w_part).alias("sqft_avg_2"),
        F.last("sqft", ignorenulls=True).over(w_hist).alias("last_known_sqft"),
        F.lag("sqft", 2, 0).over(w4).alias("prev_sqft"),
        F.ntile(4).over(w_part_2).alias("sqft_bucket"),
    )
)
