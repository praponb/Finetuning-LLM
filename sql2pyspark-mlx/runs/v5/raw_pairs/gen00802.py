from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "RETURNED")
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
    )
)
