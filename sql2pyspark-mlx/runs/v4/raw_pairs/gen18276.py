from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("kerry_events").desc())

result = (
    df_shipments
    .groupBy(
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
        F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"),
    )
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.avg(F.when(F.col("carrier") == "FedEx", F.col("cost"))).alias("fedex_avg_cost"),
        F.count_if(F.col("carrier") == "Kerry").alias("kerry_events"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .withColumn("kerry_events_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("fedex_avg_cost").desc())
)
