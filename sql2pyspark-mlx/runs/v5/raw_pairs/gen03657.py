from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") >= 1100)
    .select("id", "merchant")
    .unionByName(
        df_transactions
        .filter(F.col("amount").between(1600, 2100))
        .select("id", "merchant")
    )
    .distinct()
)
