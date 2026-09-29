from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["mobile", "store", "partner", "web"])
    .agg(F.count("amount"))
)
