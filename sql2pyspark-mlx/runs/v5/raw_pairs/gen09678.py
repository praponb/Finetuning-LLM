from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(29, 36))
    .select(
        "id",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue").desc(), F.col("revenue_per_unit").desc(), "id")
    .limit(5)
)
