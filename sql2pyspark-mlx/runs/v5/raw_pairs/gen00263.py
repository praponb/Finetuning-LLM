from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper(F.trim("supplier_name")) == "LION CITY IMPORTS")
    .select("rating")
    .orderBy(F.col("rating").desc())
)
