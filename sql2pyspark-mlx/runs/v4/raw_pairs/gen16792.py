from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "RETURNED") & F.col("order_id").isin([5, 25, 26]) & (F.col("delivered_date") >= "2024-06-01"))
    .groupBy(F.month("delivered_date").alias("delivered_month"), "status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.avg("cost").alias("avg_cost"),
    )
)
