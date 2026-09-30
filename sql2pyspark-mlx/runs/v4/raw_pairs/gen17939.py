from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") == 2023)
    .select(
        "id",
        "order_date",
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
    )
)
