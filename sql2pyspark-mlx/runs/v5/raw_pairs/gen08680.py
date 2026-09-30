from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNull())
    .select("sale_date", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("units").between(28, 33))
        .select("sale_date", "product_id")
    )
)
