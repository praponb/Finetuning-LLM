from pyspark.sql import functions as F

result = (
    df_shipments
    .select("carrier")
    .distinct()
    .crossJoin(df_subscriptions.select("plan").distinct())
)
