from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("cost").alias("value"))
    .unionByName(df_payments.select(F.col("amount").alias("value")))
)
