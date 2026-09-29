from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost")).otherwise(0)).alias("fedex_cost"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)
