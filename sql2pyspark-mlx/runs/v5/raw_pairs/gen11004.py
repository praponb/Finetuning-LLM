from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .orderBy("delivered_month")
)
