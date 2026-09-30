from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .distinct()
)
