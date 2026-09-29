from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("amount_int_2", F.col("amount").cast("int"))
)
