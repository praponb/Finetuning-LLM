from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "LOST")
    .select("status", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("status", "shipped_date")
    )
    .distinct()
)
