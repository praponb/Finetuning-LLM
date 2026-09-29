from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5]))
    .select(
        "id",
        "units",
        "revenue",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
        F.add_months("sale_date", 3).alias("sale_date_plus_months"),
    )
)
