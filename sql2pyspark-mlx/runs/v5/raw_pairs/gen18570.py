from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

metrics = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lead("cost").over(w).alias("next_cost"),
        F.row_number().over(w).alias("event_seq"),
    )
)

kept_keys = (
    metrics
    .select("carrier", "id")
    .exceptAll(
        df_shipments
        .filter(F.col("status").isin(["IN_TRANSIT", "RETURNED", "LOST"]))
        .select("carrier", "id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    kept_rows
    .select("carrier", "id")
    .subtract(
        df_shipments
        .filter(F.year("shipped_date") >= 2023)
        .select("carrier", "id")
    )
)

kept_rows_shipments = (
    kept_rows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("shipped_date") > F.to_date(F.lit("2023-07-05"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "next_cost",
        "event_seq",
        (F.col("cost") * F.col("event_seq")).alias("cost_times_event_seq"),
        F.greatest("prev_cost", "event_seq").alias("greatest_prev_cost_event_seq"),
    )
)

result = (
    cleaned
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "event_seq",
        "greatest_prev_cost_event_seq",
        F.col("min_payment_amount"),
    )
    .orderBy(F.col("greatest_prev_cost_event_seq").desc_nulls_last())
)
