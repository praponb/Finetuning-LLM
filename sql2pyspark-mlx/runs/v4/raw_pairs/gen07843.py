from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .union(df_subscriptions.filter(F.col("plan") == "basic").select("start_date"))
)
