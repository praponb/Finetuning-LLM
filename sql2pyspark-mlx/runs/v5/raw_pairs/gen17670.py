from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Tokyo"]) & ((F.col("country") != "DE") | (F.col("city") == "Berlin")))
    .select("id", "city", "rating")
)
