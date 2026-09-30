from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eu%"))
    .select("id", "rating", "city")
    .orderBy("rating", F.col("id").desc())
)
