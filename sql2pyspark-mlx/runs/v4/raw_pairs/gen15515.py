from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("email") < 12)
    .select("city")
    .subtract(df_stores.select("city"))
)
