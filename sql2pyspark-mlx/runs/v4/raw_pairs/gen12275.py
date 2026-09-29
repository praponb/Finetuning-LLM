from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.max("units").alias("max_units"),
    )
)
