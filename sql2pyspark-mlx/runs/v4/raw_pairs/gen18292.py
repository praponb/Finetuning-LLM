from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("id", "delivered_date")
    .unionByName(
        df_shipments
        .filter(F.col("carrier") == "Flash")
        .select("id", "delivered_date")
    )
    .distinct()
    .orderBy("id")
)
