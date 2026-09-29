from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount", F.round("amount", 1).alias("amount_rounded"))
    .orderBy("amount_rounded", F.col("amount").desc())
)
