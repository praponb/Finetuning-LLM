from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("order_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("order_date_plus_months", "id")
w4 = Window.partitionBy("channel", "customer_id").orderBy(F.col("first_known_shipping_fee").desc_nulls_last(), "id")

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round((F.col("amount") * 100), 2).alias("amount_scaled"),
        F.add_months("order_date", 3).alias("order_date_plus_months"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") <= F.col("s.order_date_plus_months")), "left_semi")
)

scored = (
    kept
    .select(
        "*",
        F.first("shipping_fee", ignorenulls=True).over(w).alias("first_known_shipping_fee"),
        F.sum("shipping_fee").over(w2).alias("running_shipping_fee"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank_2", F.row_number().over(w4))
    .filter(F.col("latest_rank_2") == 1)
)

result = (
    latest
    .select(
        "id",
        "status",
        "discount",
        "amount_scaled",
        "amount",
        "first_known_shipping_fee",
    )
    .unpivot(
        ids=["id", "status"],
        values=["discount", "amount_scaled", "amount", "first_known_shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
)
