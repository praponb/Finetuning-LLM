from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1700, 3500))
    .select("id", "merchant")
    .unionByName(
        df_transactions
        .filter(F.to_date("txn_time") < "2023-11-01")
        .select("id", "merchant")
    )
)
