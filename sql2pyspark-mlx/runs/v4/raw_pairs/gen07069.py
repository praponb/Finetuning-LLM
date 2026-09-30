from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
    )
)
