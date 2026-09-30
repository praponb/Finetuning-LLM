from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("L%"))
    .select("id", "city")
    .unionByName(df_suppliers.filter(F.col("rating") <= 3.8).select("id", "city"))
    .distinct()
)
