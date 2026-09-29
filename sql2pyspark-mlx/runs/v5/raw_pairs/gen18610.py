from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "New York"]) & (F.col("country") == "JP") & F.col("rating").between(1.9, 2.0))
    .select("id", "country", "city")
)
