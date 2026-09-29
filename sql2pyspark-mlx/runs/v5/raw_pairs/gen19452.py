from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("credit_limit") <= 6500) & F.col("city").isin(["Bangkok", "London"])) | F.col("email").like("%@company.co"))
    .select("id", "city", "age")
)
