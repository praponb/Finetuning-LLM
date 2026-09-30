from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.3)
    .select("id", "country")
    .intersect(df_suppliers.filter(F.col("country") == "US").select("id", "country"))
)
