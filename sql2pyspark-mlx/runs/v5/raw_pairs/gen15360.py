from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Supp%"))
    .select("id", "country")
)
