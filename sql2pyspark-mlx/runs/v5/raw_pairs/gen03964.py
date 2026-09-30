from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 5)
    .select(
        "id",
        "sale_date",
        "units",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)
