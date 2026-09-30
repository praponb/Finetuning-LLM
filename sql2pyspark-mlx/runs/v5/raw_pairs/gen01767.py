from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(13, 30) & (F.col("sale_date") > "2025-06-02") & F.col("product_id").isin([1, 2, 4, 6]))
    .select(
        "id",
        "store_id",
        "units",
        F.quarter("sale_date").alias("sale_date_quarter"),
    )
)
