from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_stores
    .select(
        "id",
        "region",
        F.sum("sqft").over(w_running).alias("running_sum_sqft"),
        F.avg("sqft").over(w_running).alias("running_avg_sqft"),
        F.max("sqft").over(w_running).alias("running_max_sqft"),
    )
)

result = (
    with_windows
    .select("id", "region", "running_sum_sqft", "running_avg_sqft", "running_max_sqft")
    .orderBy(F.col("running_sum_sqft").desc_nulls_last())
)
