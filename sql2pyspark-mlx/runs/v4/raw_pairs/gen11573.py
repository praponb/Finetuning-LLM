from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "South"]))
    .select("opened_date")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isin([5, 14, 18]))
        .select("shipped_date")
    )
    .orderBy("opened_date")
)
