from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id")
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc())
w3 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "carrier").orderBy(F.col("cost").asc_nulls_first())

with_windows = (
    df_shipments
    .withColumn("cost_bucket", F.ntile(3).over(w))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("cumulative_cost", F.sum("cost").over(w3))
    .withColumn("cost_dense_rank", F.dense_rank().over(w4))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_bucket",
        "cost_cume_dist",
        "cumulative_cost",
        "cost_dense_rank",
        F.when(F.col("status").rlike("^(IN_TRANSIT|DELIVERED)$"), "EXTENDED").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("cumulative_cost").asc_nulls_first())
)
