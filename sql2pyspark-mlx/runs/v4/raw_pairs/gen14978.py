from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status")
w2 = Window.partitionBy("status_count_cost_2_tier").orderBy(F.col("num_rows").desc())

enriched = (
    df_shipments
    .select(
        "*",
        F.count("cost").over(w_ordered).alias("status_count_cost"),
        F.count("cost").over(w_ordered).alias("status_count_cost_2"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("status_count_cost_2") >= 15, "HIGH").otherwise("LOW").alias("status_count_cost_2_tier"),
        "order_id",
        F.date_trunc("MONTH", "delivered_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("status_count_cost"), 2).alias("avg_status_count_cost"),
        F.count("*").alias("num_rows"),
    )
    .withColumn("num_rows_percent_rank", F.percent_rank().over(w2))
)
