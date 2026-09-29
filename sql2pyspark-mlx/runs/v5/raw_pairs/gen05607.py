from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc_nulls_last(), "id")

cleaned = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Kerry"]) & (F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_date_label"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

filtered = (
    cleaned
    .filter(F.col("carrier").isNotNull() & (F.col("status") == "RETURNED"))
    .withColumn("delivered_date_day", F.dayofmonth("delivered_date"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("UNKNOWN")))
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

filtered_shipments = (
    ranked
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-07-08"))) & F.col("status").isin(["RETURNED", "IN_TRANSIT"]))
    .select("*", F.lower("status").alias("status_lower"))
)

valid = (
    filtered_shipments
    .filter(~F.col("status").isin(["RETURNED", "DELIVERED"]))
    .select(
        "*",
        (F.col("row_num") / F.nullif("delivered_date_day", F.lit(0))).alias("row_num_per_delivered_date_day"),
        (F.col("row_num") * F.col("delivered_date_day")).alias("row_num_times_delivered_date_day"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "carrier",
        "row_num_per_delivered_date_day",
        "row_num_times_delivered_date_day",
    )
    .unpivot(
        ids=["id", "carrier"],
        values=["row_num_per_delivered_date_day", "row_num_times_delivered_date_day"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
