from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("adjusted_balance", F.round((F.col("balance") * 1.07), 2))
    .withColumn("status_upper", F.upper("status"))
    .orderBy("status", "customer_id")
)
