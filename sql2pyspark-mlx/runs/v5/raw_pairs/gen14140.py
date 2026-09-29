from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "revenue",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)
