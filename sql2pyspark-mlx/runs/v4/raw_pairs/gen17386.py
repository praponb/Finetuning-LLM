from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 9100)
    .select("city")
    .unionByName(df_customers.filter(~F.col("is_active")).select("city"))
)
