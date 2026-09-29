from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.28)
    .select("status")
    .unionByName(df_accounts.filter(F.col("status") == "OPEN").select("status"))
    .distinct()
)
