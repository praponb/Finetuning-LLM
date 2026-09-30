from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "cost",
        "delivered_date",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
        F.round("cost", 0).alias("cost_rounded"),
    )
    .orderBy(F.col("cost").desc(), "id")
    .limit(20)
)
