from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Fashion")
    .groupBy(
        "supplier_id",
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
    )
    .agg(
        F.count("price").alias("price_count"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("price_count") > 8)
)
