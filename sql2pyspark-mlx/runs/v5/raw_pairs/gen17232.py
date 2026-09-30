from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("paid_at_day", "id")
w3 = Window.orderBy("paid_at_day", "id")
w4 = Window.orderBy("seq_no", "id")
w_recent = Window.orderBy(F.col("row_seq").asc_nulls_first())
w_part = Window.orderBy(F.col("paid_at_day").desc(), "id")
w7 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("row_seq_percent_rank").desc())

base = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-08-23"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "status",
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
)

enriched = (
    base
    .select(
        "id",
        "paid_at_day",
        F.first("method", ignorenulls=True).over(w).alias("first_known_method"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

enriched_payments = (
    enriched
    .filter(F.year("paid_at_day") >= 2025)
    .select(
        "*",
        F.lead("row_seq", 2).over(w4).alias("next_row_seq"),
        F.percent_rank().over(w_recent).alias("row_seq_percent_rank"),
        F.row_number().over(w_part).alias("row_seq_2"),
    )
)

scored = (
    enriched_payments
    .select(
        "id",
        "paid_at_day",
        "first_known_method",
        "row_seq_percent_rank",
        "row_seq_2",
        F.first("seq_no", ignorenulls=True).over(w7).alias("first_known_seq_no"),
        F.percent_rank().over(w8).alias("row_seq_percent_rank_percent_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "paid_at_day",
        "row_seq_percent_rank",
        "row_seq_2",
        "row_seq_percent_rank_percent_rank",
    )
)
