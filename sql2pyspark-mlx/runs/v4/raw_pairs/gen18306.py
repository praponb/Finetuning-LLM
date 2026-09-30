from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 7400)
    .select("id", "age")
    .unionByName(df_customers.filter(~F.col("is_active")).select("id", "age"))
    .orderBy("id")
)
