from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["UPS", "FedEx"]), "priority").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.count("*").alias("num_shipments"),
        F.countDistinct("cost").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") < 4)
    .drop("unique_statuss")
)
