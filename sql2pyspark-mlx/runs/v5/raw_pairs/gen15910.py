from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("delivered_date") >= "2025-05-11"))
    .select(
        "*",
        F.concat("status", F.lit("/"), "carrier").alias("status_carrier"),
        F.dayofmonth("shipped_date").alias("shipped_date_day"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

result = (
    base
    .groupBy("status")
    .pivot("carrier", ["Flash", "FedEx"])
    .agg(F.max("cost"))
    .select("status", F.col("Flash").alias("flash"), F.col("FedEx").alias("fedex"))
)
