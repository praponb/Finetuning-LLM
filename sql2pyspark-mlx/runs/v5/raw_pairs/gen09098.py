from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id")
w2 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("carrier", "order_id").orderBy(F.col("shipped_date").desc(), "id")

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("status") == "DELIVERED"))
    .select(
        "*",
        F.dayofweek("shipped_date").alias("shipped_date_dayofweek"),
        F.year("shipped_date").alias("shipped_date_year"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.channel"))
)

metrics = (
    enriched
    .filter(F.col("shipped_date") > "2024-10-15")
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        "shipped_date_dayofweek",
        "shipped_date_year",
        "resolved_carrier",
        F.row_number().over(w).alias("row_seq"),
        F.avg("shipped_date_year").over(w2).alias("shipped_date_year_avg_2"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

matched_keys = (
    metrics
    .select("id", "status")
    .subtract(
        df_shipments
        .filter(~F.col("carrier").isin(["Flash", "UPS"]))
        .select("id", "status")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "delivered_date",
        "cost",
        "shipped_date_dayofweek",
        "shipped_date_year",
        "resolved_carrier",
        "shipped_date_year_avg_2",
        "event_seq",
    )
    .orderBy(F.col("shipped_date_dayofweek").desc(), "delivered_date")
)
