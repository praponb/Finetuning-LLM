from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-12-09", "2025-07-01") | (F.col("balance") < 36000))
    .withColumn("account_type_upper", F.upper("account_type"))
    .orderBy("customer_id")
)
