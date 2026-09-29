from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("credit_limit") <= 12300) & (F.upper(F.trim("name")) == "BOB SMITH")) | (F.col("age") >= 67))
)
