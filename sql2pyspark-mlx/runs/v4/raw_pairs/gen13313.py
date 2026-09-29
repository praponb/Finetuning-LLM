from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("order_id") == 4)

shipments_stats = (
    filtered_shipments
    .groupBy("status")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    shipments_stats
    .orderBy(F.col("status").desc(), F.col("total_cost").desc())
    .limit(10)
)
