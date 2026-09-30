from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date", F.lit("customer").alias("source"))
    .union(
        df_store_sales
        .filter(F.col("product_id") == 5)
        .select("sale_date", F.lit("store_sale").alias("source"))
    )
)
