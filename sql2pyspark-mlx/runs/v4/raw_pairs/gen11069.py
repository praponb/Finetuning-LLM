from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "DELIVERED")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy("flash_cost")
)
