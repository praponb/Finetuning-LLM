from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "country", "supplier_name")
    .orderBy(F.col("country").desc(), "id")
    .limit(50)
)
