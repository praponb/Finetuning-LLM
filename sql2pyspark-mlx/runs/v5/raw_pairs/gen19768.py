from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["UPS", "FedEx"]), "core").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
