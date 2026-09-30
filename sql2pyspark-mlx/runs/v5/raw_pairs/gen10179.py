from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "South", "East"]))
    .select("opened_date")
    .union(df_customers.select("signup_date"))
)
