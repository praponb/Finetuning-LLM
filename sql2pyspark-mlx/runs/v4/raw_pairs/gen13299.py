from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .select("end_date")
    .subtract(df_store_sales.select("sale_date"))
    .orderBy(F.col("end_date").desc())
)
