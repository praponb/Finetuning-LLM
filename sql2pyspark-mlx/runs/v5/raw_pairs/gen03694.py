from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
    )
    .orderBy("carrier")
)
