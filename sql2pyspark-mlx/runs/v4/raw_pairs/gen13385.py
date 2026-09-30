from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 5)
    .select(
        "id",
        "revenue",
        "product_id",
        "units",
        F.date_trunc("year", "sale_date").alias("sale_date_period"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
