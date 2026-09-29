from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 5]))
    .select("revenue")
    .subtract(
        df_transactions
        .filter(F.col("account_id").isin([19, 36]))
        .select("amount")
    )
)
