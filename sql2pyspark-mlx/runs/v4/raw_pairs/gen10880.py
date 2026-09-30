from pyspark.sql import functions as F

src = df_accounts.filter(F.col("customer_id").isin([10, 21, 30, 31]))

summary = (
    src
    .groupBy("customer_id")
    .agg(F.max("balance").alias("max_balance"))
)

result = (
    summary
    .filter(F.col("max_balance") >= 53000)
    .select("customer_id", "max_balance")
    .orderBy("customer_id")
)
