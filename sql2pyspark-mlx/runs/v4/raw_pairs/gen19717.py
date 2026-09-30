from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("is_corporate", F.when(F.col("segment") == "Corporate", 1).otherwise(0))
)
