from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-04-09").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        F.concat("carrier", F.lit(" "), "status").alias("carrier_status"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("YEAR", "delivered_date").alias("year_start"), "status")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy("num_status")
)
