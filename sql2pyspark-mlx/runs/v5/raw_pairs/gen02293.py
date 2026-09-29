from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") != "RETURNED") & (F.col("shipped_date") <= "2023-08-01") & F.col("carrier").isin(["FedEx", "UPS"]))
    .groupBy("carrier")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
    )
    .orderBy("flash_count")
)
