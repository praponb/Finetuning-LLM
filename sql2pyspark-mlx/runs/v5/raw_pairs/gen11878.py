from pyspark.sql import functions as F

result = (
    df_products
    .select(F.lit("product").alias("kind"), F.col("launch_date").alias("event_date"))
    .unionByName(
        df_employees
        .filter(~F.col("is_remote"))
        .select(F.lit("employee").alias("kind"), F.col("hire_date").alias("event_date"))
    )
)
