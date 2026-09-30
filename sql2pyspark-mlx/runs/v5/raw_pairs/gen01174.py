from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .distinct()
    .orderBy("status")
    .limit(3)
)
