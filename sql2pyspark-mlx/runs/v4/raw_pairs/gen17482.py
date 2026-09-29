from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .select(
        "*",
        F.lag("cost", 1).over(w_hist).alias("prev_cost"),
        F.ntile(10).over(w2).alias("cost_bucket"),
    )
)
