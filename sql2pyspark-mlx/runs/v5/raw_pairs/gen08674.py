from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "status",
        "shipped_date",
        "carrier",
        F.round("cost", 0).alias("cost_rounded"),
        F.round("cost", 2).alias("cost_rounded_2"),
    )
)
