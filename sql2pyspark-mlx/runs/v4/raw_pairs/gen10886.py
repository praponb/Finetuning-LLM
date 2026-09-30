from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "New York")
    .select("country")
    .unionByName(df_suppliers.filter(F.col("city") == "Chiang Mai").select("country"))
)
