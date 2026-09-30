from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "cash"]))
    .select("method")
    .unionByName(df_payments.filter(F.col("amount") < 690).select("method"))
    .distinct()
    .orderBy(F.col("method").desc())
)
