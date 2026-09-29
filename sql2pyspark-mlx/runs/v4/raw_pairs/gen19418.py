from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([12, 15, 20, 30]))
    .select("id", "opened_date", "customer_id")
    .orderBy("opened_date", "id")
    .limit(1)
)
