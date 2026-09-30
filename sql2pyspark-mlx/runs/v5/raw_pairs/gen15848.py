from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.36)
    .select("status")
    .unionByName(df_accounts.filter(F.col("customer_id").isNotNull()).select("status"))
    .distinct()
)
