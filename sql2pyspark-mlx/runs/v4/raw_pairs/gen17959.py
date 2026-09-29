from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "supplier_name", "country", "rating")
    .orderBy(F.col("rating").desc())
)
