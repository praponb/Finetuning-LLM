from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date", F.lit("product").alias("source"))
    .union(
        df_employees
        .filter(F.col("dept_id") == 10)
        .select("hire_date", F.lit("employee").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
