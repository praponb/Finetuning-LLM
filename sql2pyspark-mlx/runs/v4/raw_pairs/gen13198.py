from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_first())
w2 = Window.orderBy("cost")

with_windows = (
    df_shipments
    .withColumn("cost_percent_rank", F.percent_rank().over(w))
    .withColumn("cost_percent_rank_2", F.percent_rank().over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_percent_rank_2",
    )
    .orderBy(F.col("shipped_date").desc(), "status")
)
