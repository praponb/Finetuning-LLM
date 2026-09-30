from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment")
    .distinct()
    .crossJoin(df_sessions.select("device").distinct())
    .orderBy("segment", "device")
)
