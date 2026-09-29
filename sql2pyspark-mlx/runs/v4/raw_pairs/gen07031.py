from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("cost").desc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_first())
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("cost").desc())
w4 = Window.partitionBy("status", "carrier").orderBy(F.col("cost").desc_nulls_first())

enriched = (
    df_shipments
    .withColumn("cost_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("cost_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("cost_percent_rank", F.percent_rank().over(w4))
)

result = (
    enriched
    .groupBy("status")
    .agg(
        F.percentile_approx("cost_cume_dist", 0.5).alias("cost_cume_dist_p50"),
        F.count_if(F.col("status") == "DELIVERED").alias("delivered_events"),
        F.avg(F.when(F.col("status") == "IN_TRANSIT", F.col("cost_percent_rank"))).alias("in_transit_avg_cost_percent_rank"),
        F.percentile_approx("cost_dense_rank", [0.25, 0.5, 0.75]).alias("cost_dense_rank_quartiles"),
        F.count("*").alias("event_count"),
        F.max("cost_percent_rank").alias("max_cost_percent_rank"),
    )
    .orderBy("delivered_events")
)
