from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-04-15").cast("date")) & F.col("brand").isNotNull())
    .withColumn("stock_qty_filled", F.coalesce("stock_qty", F.lit(0)))
    .withColumn("launch_date_month_end", F.last_day("launch_date"))
)

result = (
    filtered
    .select("supplier_id", "id")
    .subtract(
        df_products
        .filter(F.col("category").isin(["Grocery", "Home", "Books"]))
        .select("supplier_id", "id")
    )
)
