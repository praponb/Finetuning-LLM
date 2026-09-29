from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("order_date").between("2023-05-01", "2023-07-03"))
    .select(
        "*",
        F.add_months("order_date", 6).alias("order_date_plus_months"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

result = (
    valid
    .select("channel")
    .subtract(
        df_orders
        .filter(F.col("amount").between(1060, 1480))
        .select("channel")
    )
)
