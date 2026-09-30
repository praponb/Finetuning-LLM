from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "CANCELLED"]))
    .select("amount")
    .intersect(
        df_store_sales
        .filter(F.col("sale_date") >= "2025-07-01")
        .select("revenue")
    )
)
