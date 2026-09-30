from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([3, 8, 12]))
    .select("age", "segment")
    .unionByName(df_customers.filter(F.col("age") == 52).select("age", "segment"))
    .distinct()
    .orderBy("segment")
)
