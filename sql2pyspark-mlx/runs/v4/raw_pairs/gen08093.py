from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("discount") < 0.26) & F.col("shipping_fee").between(15, 17)) | (F.col("channel") != "web"))
    .withColumn("adjusted_discount", F.round((F.col("discount") * 100), 2))
)
