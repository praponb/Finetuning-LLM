from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 30)
    .select(
        "id",
        "units",
        "revenue",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
