from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "DE")
    .select("city")
    .intersect(df_stores.filter(F.col("sqft").between(5016, 11213)).select("city"))
)
