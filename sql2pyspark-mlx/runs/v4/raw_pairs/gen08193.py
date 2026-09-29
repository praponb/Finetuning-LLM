from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city")
    .subtract(df_suppliers.filter(F.col("rating") <= 3.0).select("country", "city"))
)
