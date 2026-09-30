from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNull())
    .select("channel", "status")
    .distinct()
)
