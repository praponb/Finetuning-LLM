from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("shipped_date", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())

metrics = (
    df_shipments
    .withColumn("running_cost", F.sum("cost").over(w))
    .withColumn("prev_cost", F.lag("cost", 1).over(w2))
    .withColumn("cost_cume_dist", F.cume_dist().over(w3))
)

result = (
    metrics
    .groupBy(F.date_trunc("WEEK", "delivered_date").alias("week_start"))
    .agg(
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
