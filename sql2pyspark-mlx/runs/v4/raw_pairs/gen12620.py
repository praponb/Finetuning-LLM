from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["partner", "store", "mobile"])
    .agg(F.sum("amount"))
)
