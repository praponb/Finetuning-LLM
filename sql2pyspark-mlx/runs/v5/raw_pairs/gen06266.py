from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel")
    .distinct()
    .orderBy("channel")
)
