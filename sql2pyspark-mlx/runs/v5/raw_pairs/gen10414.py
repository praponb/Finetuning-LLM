from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST"]) & F.col("carrier").isNull())
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
    )
)
