from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.year("sale_date").alias("sale_year"),
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
    )
    .agg(F.sum("units").alias("total_units"))
    .orderBy(F.col("total_units").desc())
)
