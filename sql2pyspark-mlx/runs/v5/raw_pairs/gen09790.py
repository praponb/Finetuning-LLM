from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
    )
    .orderBy(F.col("id").desc())
)
