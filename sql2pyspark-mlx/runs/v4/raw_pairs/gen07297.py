from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 3900) & (F.col("amount") > 2700))
    .select("amount")
    .orderBy(F.col("amount").desc())
)
