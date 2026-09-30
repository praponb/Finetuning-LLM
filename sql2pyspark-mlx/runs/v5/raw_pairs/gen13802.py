from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-05-10"))) & F.col("units").isNotNull())
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
    )
)

result = (
    cleaned
    .select("id")
    .exceptAll(
        df_store_sales
        .filter(F.col("store_id").isin([3, 4, 6]))
        .select("id")
    )
)
