from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")

deduped = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "shipped_date", "delivered_date", "cost", "status")
)

result = (
    deduped
    .groupBy(F.date_trunc("week", "shipped_date").alias("week_start"))
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.stddev_samp("cost").alias("cost_stddev_2"),
    )
)
