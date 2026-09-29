from pyspark.sql import functions as F

result = (
    df_customers
    .select("age", "country")
    .orderBy(F.col("age").desc())
)
