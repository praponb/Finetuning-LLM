from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 24)
    .select("status")
    .intersect(df_accounts.select("status"))
    .orderBy(F.col("status").desc())
)
