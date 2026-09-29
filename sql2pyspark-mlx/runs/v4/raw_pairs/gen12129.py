from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .pivot("carrier", ["FedEx", "Kerry", "DHL"])
    .agg(F.sum("cost").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "order_id",
        F.col("FedEx_total").alias("fedex_total"),
        F.col("FedEx_cnt").alias("fedex_cnt"),
        F.col("Kerry_total").alias("kerry_total"),
        F.col("Kerry_cnt").alias("kerry_cnt"),
        F.col("DHL_total").alias("dhl_total"),
        F.col("DHL_cnt").alias("dhl_cnt"),
    )
)
