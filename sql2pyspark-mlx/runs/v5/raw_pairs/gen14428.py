from pyspark.sql import functions as F

by_dimension = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .cube(F.col("o.status").alias("order_status"), F.col("e.carrier").alias("carrier"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.cost")).alias("max_cost"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    by_dimension
    .select(
        "order_status",
        "carrier",
        "cnt",
        "max_cost",
        "level_id",
        F.when(F.col("carrier").rlike("^(Flash|UPS)$"), "CORE").when(F.col("carrier").rlike("^(DHL|FedEx)$"), "EXTENDED").otherwise("GROUP_B").alias("carrier_zone"),
    )
)
