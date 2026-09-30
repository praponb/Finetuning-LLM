from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([11, 19]))
    .select("opened_date", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("customer_id") == 31)
        .select("opened_date", "customer_id")
    )
)
