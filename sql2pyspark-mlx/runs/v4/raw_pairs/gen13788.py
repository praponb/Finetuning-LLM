from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "cost",
        "status",
        "shipped_date",
        F.trim("status").alias("clean_status"),
        F.date_add("delivered_date", 21).alias("delivered_date_plus_21d"),
    )
    .orderBy("clean_status")
)
