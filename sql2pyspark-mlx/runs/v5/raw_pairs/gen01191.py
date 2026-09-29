from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-09-14").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.round("cost", 1).alias("cost_rounded"),
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
    )
)

result = (
    valid
    .select("carrier")
    .exceptAll(
        df_shipments
        .filter(F.year("delivered_date") == 2023)
        .select("carrier")
    )
)
