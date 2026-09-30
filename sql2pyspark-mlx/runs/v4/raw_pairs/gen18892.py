from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 32)
    .groupBy("status")
    .pivot("carrier", ["DHL", "Kerry"])
    .agg(F.max("cost"))
)
