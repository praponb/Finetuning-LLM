from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .pivot("carrier", ["Kerry", "UPS", "Flash", "DHL"])
    .agg(F.max("cost"))
    .orderBy("delivered_month")
)
