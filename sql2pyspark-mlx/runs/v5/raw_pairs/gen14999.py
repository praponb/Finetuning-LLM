from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 680)
    .withColumn("status_method_key", F.concat_ws(",", "status", "method"))
    .orderBy(F.col("order_id").desc(), F.col("id").desc())
)
