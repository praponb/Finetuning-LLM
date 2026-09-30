from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last(), "id")

latest = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "product_id", "sale_date", "units", "revenue")
)

base = (
    latest
    .filter(F.col("revenue") >= 2900)
    .select(
        "*",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

valid = (
    base
    .filter((F.col("sale_date") >= F.lit("2023-06-04").cast("date")) & F.col("product_id").isin([2, 3, 9, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "days_since_sale_date",
        F.coalesce("days_since_sale_date", F.lit(0)).alias("days_since_sale_date_filled"),
        (F.col("days_since_sale_date") - F.col("units")).alias("days_since_sale_date_minus_units"),
    )
)

latest_store_sales = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "days_since_sale_date_filled",
    )
    .orderBy(F.col("product_id").desc_nulls_first(), "sale_date")
)
