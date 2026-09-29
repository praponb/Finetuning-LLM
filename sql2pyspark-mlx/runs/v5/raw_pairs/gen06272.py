from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("delivered_date")

deduped = (
    df_shipments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .withColumn("week_start", F.date_trunc("week", "delivered_date"))
    .withColumn("quarter_start", F.date_trunc("quarter", "delivered_date"))
    .cube("week_start", "quarter_start")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost_2"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_cost") < 1000000)
    .orderBy("week_start")
)
