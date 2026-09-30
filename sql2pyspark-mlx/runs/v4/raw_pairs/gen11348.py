from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("cost", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("carrier").orderBy("cost", "id")

result = (
    df_shipments
    .select(
        "*",
        F.first("carrier", ignorenulls=True).over(w).alias("first_known_carrier"),
        F.avg("cost").over(w2).alias("cost_avg_5_prior"),
        F.stddev_samp("cost").over(w3).alias("status_stddev_samp_cost"),
        F.ntile(4).over(w4).alias("cost_bucket"),
    )
)
