from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("shipped_date") >= F.to_date(F.lit("2023-04-05")))
    .select(
        "*",
        F.when(F.col("status").isin(["RETURNED", "DELIVERED"]), "Group A").otherwise("Other").alias("status_group"),
    )
)

result = (
    base
    .groupBy("order_id")
    .pivot("carrier", ["Kerry", "UPS", "DHL"])
    .agg(F.max("cost").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "order_id",
        F.col("Kerry_amount").alias("kerry_amount"),
        F.col("Kerry_n").alias("kerry_n"),
        F.col("UPS_amount").alias("ups_amount"),
        F.col("UPS_n").alias("ups_n"),
        F.col("DHL_amount").alias("dhl_amount"),
        F.col("DHL_n").alias("dhl_n"),
    )
)
