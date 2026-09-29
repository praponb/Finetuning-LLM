from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.count("cost").alias("cost_count"),
        F.max("shipped_date").alias("latest_shipped_date"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") >= 310)
    .drop("total_cost")
)
