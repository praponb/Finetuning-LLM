from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.quarter("sale_date").alias("sale_quarter"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .orderBy(F.col("sale_quarter").desc(), "latest_sale_date", "sale_year")
    .limit(10)
)
