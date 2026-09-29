from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") != 57)
    .agg(F.countDistinct("credit_limit").alias("unique_countrys"))
)
