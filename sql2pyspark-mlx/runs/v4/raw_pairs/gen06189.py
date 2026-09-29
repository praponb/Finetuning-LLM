from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-04-01").cast("date")) & F.col("revenue").between(1300, 2900))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.round((F.col("revenue") * 100), 2).alias("revenue_adjusted"),
    )
)

result = (
    cleaned
    .select("product_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.col("revenue") < 1700)
        .select("product_id", "id")
    )
)
