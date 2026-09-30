from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNull() | (F.length("supplier_name") < 8))
    .select("id", "city", "country")
)
