from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "store_id",
        "revenue",
        F.date_sub("sale_date", 73).alias("sale_date_minus_73d"),
    )
)
