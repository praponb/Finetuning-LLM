from pyspark.sql import functions as F

max_sale_date = df_store_sales.agg(F.max("sale_date")).first()[0]

result = (
    df_store_sales
    .filter(F.col("sale_date") > max_sale_date)
    .select("id", "sale_date")
    .orderBy(F.col("id").desc())
)
