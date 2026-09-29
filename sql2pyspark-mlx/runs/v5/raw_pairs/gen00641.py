from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2023-04-01")
    .select(
        "product_id",
        "sale_date",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)
