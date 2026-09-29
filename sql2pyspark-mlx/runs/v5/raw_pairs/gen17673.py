from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("cost").asc_nulls_last())

metrics = (
    df_shipments
    .withColumn("running_avg_cost", F.avg("cost").over(w))
    .withColumn("running_count_cost", F.count("cost").over(w))
    .withColumn("running_sum_cost", F.sum("cost").over(w))
    .withColumn("running_max_cost", F.max("cost").over(w))
    .withColumn("cost_rank", F.rank().over(w2))
)

result = (
    metrics
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        "running_avg_cost",
        "running_count_cost",
        "running_sum_cost",
        "cost_rank",
    )
)
