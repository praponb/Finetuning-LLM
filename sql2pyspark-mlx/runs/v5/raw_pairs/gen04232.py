from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier")
w2 = Window.partitionBy("status").orderBy("cost", "id")
w_running = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-2, -1)

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.ntile(10).over(w2).alias("cost_bucket"),
        F.avg("cost").over(w_running).alias("cost_avg_2_prior"),
    )
)
