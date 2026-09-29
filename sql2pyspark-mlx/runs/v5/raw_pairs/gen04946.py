from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([4, 7, 8]) & (F.length("email") < 5))
)
