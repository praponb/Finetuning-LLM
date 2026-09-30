from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1200, 3400))
    .select("merchant", F.round("amount", 2).alias("amount_rounded"))
    .orderBy("amount_rounded")
)
