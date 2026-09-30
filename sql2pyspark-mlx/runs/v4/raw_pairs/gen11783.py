from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", F.lit("customer").alias("source"))
    .unionByName(df_suppliers.select("city", F.lit("supplier").alias("source")))
    .orderBy("city")
)
