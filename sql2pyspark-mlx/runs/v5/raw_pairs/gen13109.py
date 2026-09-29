from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") >= 2025)
    .select("status")
    .intersect(
        df_orders
        .filter(F.col("order_date").between("2023-07-01", "2025-08-24"))
        .select("status")
    )
    .orderBy("status")
)
