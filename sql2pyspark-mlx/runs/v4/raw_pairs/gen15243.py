from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 40]))
    .select("merchant")
    .unionByName(
        df_transactions
        .filter(~F.col("merchant").isin(["7-Eleven", "Shell", "Amazon"]))
        .select("merchant")
    )
    .distinct()
)
