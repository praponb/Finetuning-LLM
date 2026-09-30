from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([19, 27, 37]))
    .select("id", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("carrier") == "Flash")
        .select("id", "shipped_date")
    )
    .distinct()
    .orderBy("shipped_date")
)
