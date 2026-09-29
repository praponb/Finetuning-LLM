from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "revenue",
        "sale_date",
        "units",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
    .orderBy("sale_date", "revenue")
)
