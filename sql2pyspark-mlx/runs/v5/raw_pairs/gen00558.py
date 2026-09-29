from pyspark.sql import functions as F

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-07-04").cast("date")) & F.col("supplier_id").isNotNull())
    .select("*", (F.col("cost") + F.col("stock_qty")).alias("cost_plus_stock_qty"))
)

result = (
    valid
    .select("supplier_id", "brand")
    .intersectAll(
        df_products
        .filter(F.col("cost").between(530, 660))
        .select("supplier_id", "brand")
    )
)
