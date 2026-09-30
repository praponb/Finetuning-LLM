from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 51)
    .select("order_id")
    .unionByName(
        df_shipments
        .filter(F.month("delivered_date").isin([2, 6, 9]))
        .select("order_id")
    )
    .distinct()
    .orderBy("order_id")
)
