from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.year("delivered_date") >= 2025)
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.concat_ws("/", "status", "carrier").alias("status_carrier_key"),
        F.when(F.col("cost") >= 104, "High").when(F.col("cost") >= 19, "Medium").otherwise("Low").alias("cost_band"),
    )
)

result = (
    cleaned
    .select("id")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date").between("2024-04-23", "2025-10-26"))
        .select("id")
    )
)
