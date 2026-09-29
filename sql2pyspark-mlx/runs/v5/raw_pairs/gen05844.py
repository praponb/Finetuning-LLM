from pyspark.sql import functions as F

result = (
    df_shipments
    .select("order_id", "id")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("order_id", "id")
    )
)
