from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.4, 4.2))
    .select("city")
    .subtract(df_stores.filter(F.col("region") == "West").select("city"))
)
