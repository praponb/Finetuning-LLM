from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") != "mobile")
    .select("status")
    .unionByName(df_accounts.select("status"))
    .distinct()
)
