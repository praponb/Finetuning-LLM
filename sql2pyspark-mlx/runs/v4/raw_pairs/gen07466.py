from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-18").cast("date")) & F.col("region").isNotNull() & ~F.col("city").isin(["Berlin", "Singapore"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
)

scored = (
    base
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

with_last = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date").alias("order_order_date"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.channel"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "opened_date_label",
        "last_known_region",
        "order_order_date",
        "order_shipping_fee",
        F.when(F.col("city").rlike("^(London|Bangkok)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc_nulls_first())
)
