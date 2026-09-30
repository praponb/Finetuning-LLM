from pyspark.sql import functions as F

result = df_customers.agg(F.min("signup_date").alias("first_signup_date"))
