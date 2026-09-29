from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2400, 3200) & (F.col("units") <= 14))
    .select(
        "id",
        "sale_date",
        "store_id",
        "units",
        F.round("revenue", 0).alias("revenue_rounded"),
    )
    .orderBy(F.col("store_id").desc())
)
