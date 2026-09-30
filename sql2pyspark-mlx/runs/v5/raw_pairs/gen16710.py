from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .distinct()
    .crossJoin(df_stores.select("city").distinct())
)
