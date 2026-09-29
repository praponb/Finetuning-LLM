from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2025)
    .select("cost")
    .subtract(
        df_orders
        .filter(F.col("order_date") > "2023-10-01")
        .select(F.col("shipping_fee").alias("cost"))
    )
    .orderBy(F.col("cost").desc())
)
