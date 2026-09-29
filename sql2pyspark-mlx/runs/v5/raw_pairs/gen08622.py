from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-08-22").cast("date")) & F.col("brand").isNotNull())
    .select("*", F.lower("product_name").alias("product_name_lower"))
)

result = (
    cleaned
    .select("category", "brand")
    .subtract(
        df_products
        .filter(F.col("launch_date") <= "2023-11-08")
        .select("category", "brand")
    )
)
