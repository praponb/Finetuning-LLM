from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city")
    .unionByName(df_suppliers.filter(F.col("rating") > 2.6).select("country", "city"))
    .distinct()
    .orderBy("country")
)
