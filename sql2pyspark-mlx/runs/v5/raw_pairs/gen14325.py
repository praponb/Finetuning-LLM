from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .unionByName(df_stores.select("city"))
    .distinct()
)
