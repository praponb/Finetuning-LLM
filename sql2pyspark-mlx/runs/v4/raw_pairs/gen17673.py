from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "store_id",
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
    )
)
