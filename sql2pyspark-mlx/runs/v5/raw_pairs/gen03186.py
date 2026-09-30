from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1300, 3200))
    .select("merchant")
    .distinct()
)
