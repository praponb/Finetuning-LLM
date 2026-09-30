from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w3 = Window.orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.orderBy("sale_date", "id")
w5 = Window.partitionBy("region").orderBy(F.col("sale_date").desc(), "id")
w6 = Window.partitionBy("region", "city").orderBy("opened_date_plus_months", "id")
w7 = Window.partitionBy("region").orderBy("sale_date", "id")

eligible = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.opened_date")), "left_semi")
)

ranked = (
    eligible
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    ranked
    .filter((F.col("opened_date") > F.lit("2024-02-19").cast("date")) & F.col("region").isNotNull())
    .withColumn("city_lower", F.lower("city"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date")), "left_anti")
)

valid_stores = (
    not_flagged
    .filter((F.col("opened_date") >= F.lit("2023-11-16").cast("date")) & F.col("region").isNotNull() & F.col("store_name").isNull())
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 12))
    .withColumn("region_filled", F.coalesce("region", F.lit("NONE")))
)

with_last = (
    valid_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

scored = (
    with_last
    .withColumn("sqft_avg_5", F.avg("sqft").over(w3))
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w_ordered))
)

result = (
    scored
    .select(
        "*",
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("seq_no"),
        F.lag("sqft", 2).over(w7).alias("prev_sqft_2"),
    )
)
