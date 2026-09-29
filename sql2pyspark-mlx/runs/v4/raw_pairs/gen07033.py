from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
    )
    .orderBy(F.col("lost_count").asc_nulls_last())
)
