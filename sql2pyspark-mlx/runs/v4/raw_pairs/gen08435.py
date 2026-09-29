from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", F.lit("department").alias("source"))
    .union(df_suppliers.select("city", F.lit("supplier").alias("source")))
    .distinct()
)
