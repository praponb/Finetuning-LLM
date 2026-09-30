from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        F.year("sale_date").alias("sale_date_year"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
    .orderBy("revenue", "id")
    .limit(5)
)
