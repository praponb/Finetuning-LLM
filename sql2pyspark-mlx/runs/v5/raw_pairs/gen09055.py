from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 12)
    .select("order_id", "status")
    .intersect(
        df_shipments
        .filter(F.col("cost").between(30, 51))
        .select("order_id", "status")
    )
    .orderBy("status")
)
