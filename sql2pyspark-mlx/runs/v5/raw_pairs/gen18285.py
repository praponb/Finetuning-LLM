from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .pivot("channel", ["web", "store", "mobile"])
    .agg(F.sum("discount"))
)
