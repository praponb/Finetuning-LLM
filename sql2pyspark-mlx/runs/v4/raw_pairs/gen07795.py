from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 1180) | F.col("order_id").isNotNull())
    .orderBy(F.col("amount").desc())
)
