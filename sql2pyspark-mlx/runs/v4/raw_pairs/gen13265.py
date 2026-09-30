from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.orderBy("cost", "id")

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.date_format("shipped_date", "yyyy-MM-dd").alias("shipped_date_label"),
    )
)

result = (
    cleaned
    .withColumn("cost_avg_2", F.avg("cost").over(w))
    .withColumn("cost_bucket", F.ntile(4).over(w_grp))
)
