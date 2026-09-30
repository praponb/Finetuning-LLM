from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)
