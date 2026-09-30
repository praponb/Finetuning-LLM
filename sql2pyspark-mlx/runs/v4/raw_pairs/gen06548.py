from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(df_subscriptions.filter(F.year("end_date") == 2024).select("status"))
    .orderBy("status")
)
