from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date", F.lit("employee").alias("source"))
    .union(
        df_store_sales
        .select("sale_date", F.lit("store_sale").alias("source"))
    )
)
