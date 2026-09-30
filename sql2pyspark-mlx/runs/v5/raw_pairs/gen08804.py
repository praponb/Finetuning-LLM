from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "revenue",
        "store_id",
        "units",
        F.least("revenue", "units").alias("least_revenue_units"),
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
)
