from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~((F.col("credit_limit") < 13800) & (F.length("email") > 10)))
    .withColumn("email_title", F.initcap(F.trim("email")))
)
