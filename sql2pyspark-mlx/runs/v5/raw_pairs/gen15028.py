from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date_month_end").desc_nulls_last(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("opened_date_month_end").desc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w5 = Window.orderBy("discount", "id")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-09-03").cast("date")) & (~F.col("region").eqNullSafe(F.lit("South"))))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.upper("city").alias("city_upper"),
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked_stores = (
    ranked
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_name", "city_upper", "opened_date_month_end")
)

with_last = (
    ranked_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", F.col("c.sale_date"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

result = (
    latest_match
    .select(
        "id",
        "sale_date",
        "discount",
        F.lag("discount").over(w5).alias("prev_discount"),
        F.lead("discount").over(w5).alias("next_discount"),
        F.row_number().over(w5).alias("row_seq"),
    )
)
