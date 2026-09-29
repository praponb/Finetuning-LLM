from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 10)
    .select("channel", "status")
    .distinct()
)
