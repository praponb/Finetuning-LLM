from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Jac%") | ~F.col("is_active"))
    .select("id", "age", "segment")
)
