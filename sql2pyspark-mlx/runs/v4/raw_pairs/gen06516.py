from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.col("shipped_date").between("2023-04-01", "2024-12-01") & (F.col("shipped_date") < "2023-06-01")))
    .select(
        "id",
        "delivered_date",
        "order_id",
        F.date_add("delivered_date", 22).alias("delivered_date_plus_22d"),
    )
    .orderBy("delivered_date")
)
