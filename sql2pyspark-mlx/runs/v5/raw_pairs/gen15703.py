from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "Corporate") | F.col("is_active"))
    .select("id", "age", "segment", "name")
    .orderBy(F.col("age").desc())
)
