from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1900)
    .select("id", "txn_type")
    .subtract(
        df_transactions
        .filter(~F.col("merchant").isin(["7-Eleven", "Grab", "Starbucks"]))
        .select("id", "txn_type")
    )
)
