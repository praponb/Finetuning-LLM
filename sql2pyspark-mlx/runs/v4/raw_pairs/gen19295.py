from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 55)
    .orderBy(F.col("name").desc())
)
