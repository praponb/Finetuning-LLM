from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(20, 21))
    .select("sale_date", F.lit("store_sale").alias("source"))
    .union(
        df_employees
        .filter(F.month("hire_date").isin([2, 5, 8]))
        .select("hire_date", F.lit("employee").alias("source"))
    )
    .distinct()
    .orderBy("sale_date")
)
