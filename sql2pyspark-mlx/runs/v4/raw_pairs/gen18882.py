from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "store_id",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
    )
    .orderBy(F.col("store_id").desc(), F.col("id").desc())
)
