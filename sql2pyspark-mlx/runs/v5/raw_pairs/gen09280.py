from pyspark.sql import functions as F

max_age = df_customers.agg(F.max("age")).first()[0]

result = (
    df_customers
    .filter(F.col("age") >= max_age)
    .select("id", "age", "city", "name")
)
