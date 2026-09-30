from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 3100)
    .select(
        "id",
        "store_id",
        "revenue",
        "sale_date",
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)
