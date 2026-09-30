from pyspark.sql import functions as F

result = (
    df_payments
    .select("method")
    .distinct()
    .crossJoin(df_shipments.select("carrier").distinct())
    .orderBy("method", "carrier")
)
