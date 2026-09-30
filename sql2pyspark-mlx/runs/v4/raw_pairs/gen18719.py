from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-06-01", "2025-07-03"))
    .select("shipped_date", "order_id")
    .intersect(
        df_shipments
        .filter(F.col("cost").between(70, 89))
        .select("shipped_date", "order_id")
    )
    .orderBy("order_id")
)
