from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel")
    .distinct()
    .crossJoin(df_shipments.select("carrier").distinct())
    .orderBy("channel", "carrier")
)
