from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .unionByName(df_stores.filter(F.year("opened_date") == 2023).select("city"))
)
