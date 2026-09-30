from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-12-18").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.date_trunc("week", "delivered_date").alias("delivered_date_period"),
        F.add_months("delivered_date", 12).alias("delivered_date_plus_months"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.avg("cost").over(w).alias("carrier_avg_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
    )
)

matched_keys = (
    enriched
    .select("id", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("status") == "DELIVERED")
        .select("id", "carrier")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("carrier", "cost", "delivered_date_plus_months", "carrier_avg_cost")
    .orderBy("cost")
)
