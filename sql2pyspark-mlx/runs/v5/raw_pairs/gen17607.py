from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 31)
    .select("sale_date")
    .union(df_subscriptions.select("start_date"))
    .distinct()
)
