from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.lit("customer").alias("origin"), "city")
    .unionByName(df_stores.select(F.lit("store").alias("origin"), "city"))
    .distinct()
)
