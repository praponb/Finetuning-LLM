from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .distinct()
    .crossJoin(df_sessions.select("device").distinct())
    .orderBy("country", "device")
)
