from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2024-07-01")
    .select(
        "id",
        "store_id",
        F.date_trunc("MONTH", "sale_date").alias("sale_date_period"),
    )
)
