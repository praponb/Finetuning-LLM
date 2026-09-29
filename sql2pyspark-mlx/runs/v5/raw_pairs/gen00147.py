from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "sale_date",
        F.col("revenue").cast("int").alias("revenue_int"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
    .orderBy("sale_date")
)
