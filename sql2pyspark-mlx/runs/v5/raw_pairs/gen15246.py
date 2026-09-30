from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 2, 3]))
    .select("customer_id")
    .unionByName(df_accounts.filter(F.col("balance") <= 39000).select("customer_id"))
    .distinct()
    .orderBy("customer_id")
)
