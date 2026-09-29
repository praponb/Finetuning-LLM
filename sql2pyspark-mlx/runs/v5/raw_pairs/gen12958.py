from pyspark.sql import functions as F

result = df_customers.select("id", "city", "email", "credit_limit")
