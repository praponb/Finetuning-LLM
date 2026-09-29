from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2024-04-20").cast("date"))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
)

result = (
    cleaned
    .select("store_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units") == 12)
        .select("store_id", "id")
    )
)
