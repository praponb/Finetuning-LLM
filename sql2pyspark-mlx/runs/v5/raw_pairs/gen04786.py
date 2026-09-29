from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .distinct()
    .orderBy("city")
    .limit(1)
)
