from pyspark.sql import functions as F

max_rating = df_suppliers.agg(F.max("rating")).first()[0]

result = (
    df_suppliers
    .filter(F.col("rating") >= max_rating)
    .select("id", "rating", "supplier_name")
)
