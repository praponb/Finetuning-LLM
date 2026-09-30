from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.add_months("sale_date", 12).alias("sale_date_plus_months"),
    )
    .orderBy("sale_date")
)
