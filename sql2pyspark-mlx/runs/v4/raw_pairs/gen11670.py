from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~((F.col("credit_limit") >= 11300) & (F.col("age") == 53)))
    .select("name", "age")
)
