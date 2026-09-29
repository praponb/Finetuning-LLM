from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.min("sale_date").alias("first_sale_date"),
    )
    .orderBy("sale_year")
    .limit(5)
)
