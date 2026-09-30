from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 25)
    .select(
        "id",
        "product_id",
        "revenue",
        "units",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
