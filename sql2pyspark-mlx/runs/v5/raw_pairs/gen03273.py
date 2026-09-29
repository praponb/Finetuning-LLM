from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("brand") == "Umbrella")
    .withColumn("product_name_upper", F.upper("product_name"))
)

agg = (
    recent_products
    .groupBy(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        "supplier_id",
    )
    .agg(F.min("stock_qty").alias("min_stock_qty"))
)

result = (
    agg
    .select("launch_month", "supplier_id", "min_stock_qty")
    .orderBy(F.col("supplier_id").desc(), F.col("min_stock_qty").desc())
)
