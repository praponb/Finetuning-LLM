from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("age") == 36) & F.col("name").like("%Sri%")) | F.lower("name").like("%liv%"))
    .groupBy("city")
    .agg(F.sum("age").alias("total_age"))
)
