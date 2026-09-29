from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .subtract(df_stores.filter(F.col("region") == "North").select("city"))
)
