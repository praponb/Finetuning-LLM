from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount")
    .subtract(df_shipments.select("cost"))
    .orderBy(F.col("amount").desc())
)
