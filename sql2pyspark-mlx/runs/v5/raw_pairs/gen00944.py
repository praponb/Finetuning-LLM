from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .union(df_store_sales.filter(F.col("store_id") == 1).select("sale_date"))
    .distinct()
    .orderBy("end_date")
)
