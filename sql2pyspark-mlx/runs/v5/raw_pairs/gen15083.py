from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Berlin") | (F.col("rating") <= 4.1))
    .select("id", "rating")
)
