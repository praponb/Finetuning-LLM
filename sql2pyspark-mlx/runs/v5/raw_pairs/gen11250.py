from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", "country")
    .subtract(df_suppliers.select("city", "country"))
    .orderBy("country")
)
