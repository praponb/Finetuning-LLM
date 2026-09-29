from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "Flash", "FedEx"]))
    .groupBy("carrier")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.avg("cost").alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
        F.countDistinct("cost").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") < 3)
    .drop("unique_orders")
    .orderBy("avg_cost")
)
