from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") <= 25)
        .select("product_id")
    )
)
