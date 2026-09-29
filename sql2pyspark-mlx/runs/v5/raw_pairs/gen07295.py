from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_rows"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("distinct_status") >= 1)
)
