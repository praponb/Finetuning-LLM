from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("days_delivered_date_to_shipped_date").desc_nulls_first(), "id")

base = (
    df_shipments
    .filter(F.col("delivered_date") >= F.to_date(F.lit("2023-06-17")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

filtered = (
    base
    .filter(F.col("shipped_date") > F.lit("2023-03-25").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "days_delivered_date_to_shipped_date",
        F.when(F.col("carrier").isin(["Flash", "Kerry"]), "Tier 1").otherwise("Other").alias("carrier_group"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "days_delivered_date_to_shipped_date",
    )
)

valid = (
    ranked
    .filter(F.col("days_delivered_date_to_shipped_date") < 28)
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "days_delivered_date_to_shipped_date",
        F.col("days_delivered_date_to_shipped_date").cast("double").alias("days_delivered_date_to_shipped_date_str"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("YEAR", "delivered_date").alias("year_start"))
    .agg(
        F.round(F.sum("days_delivered_date_to_shipped_date") / F.nullif(F.count(F.when(F.col("carrier") == "UPS", 1)), F.lit(0)), 2).alias("days_delivered_date_to_shipped_date_per_ups"),
        F.percentile_approx("days_delivered_date_to_shipped_date_str", [0.25, 0.5, 0.75]).alias("days_delivered_date_to_shipped_date_str_quartiles"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)
