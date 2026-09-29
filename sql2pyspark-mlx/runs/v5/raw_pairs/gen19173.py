from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "sale_date",
        "units",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
    .orderBy(F.col("sale_date_label").desc())
)
