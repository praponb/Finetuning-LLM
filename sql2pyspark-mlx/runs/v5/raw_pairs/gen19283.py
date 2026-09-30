from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.max("cost").alias("max_cost"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .orderBy("max_cost")
)
