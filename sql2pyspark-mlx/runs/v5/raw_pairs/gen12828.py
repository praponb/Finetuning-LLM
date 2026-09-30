from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(~F.col("status").isin(["RETURNED", "DELIVERED", "LOST"]))

agg = (
    valid_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["DHL", "UPS"]), "core").otherwise("other").alias("carrier_group"),
    )
    .agg(F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"))
)

result = (
    agg
    .select("carrier_group", "flash_count")
    .orderBy("flash_count", F.col("carrier_group").desc())
)
