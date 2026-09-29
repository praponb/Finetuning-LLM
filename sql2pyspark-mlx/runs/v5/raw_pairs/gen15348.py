from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(28, 44) & (F.year("delivered_date") >= 2023) & (F.col("cost") <= 80))
    .select(
        "order_id",
        "cost",
        F.date_sub("shipped_date", 66).alias("shipped_date_minus_66d"),
    )
)
