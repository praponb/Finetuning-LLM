from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .subtract(df_stores.filter(F.col("sqft") != 4893).select("city"))
)
