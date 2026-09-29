from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("sale_date_plus_10d", "id")
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("store_id").orderBy(F.col("next_units").asc_nulls_first(), "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("event_seq").desc_nulls_first(), "id")

base = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-05-23").cast("date"))
    .withColumn("sale_date_plus_10d", F.date_add("sale_date", 10))
)

with_windows = (
    base
    .select(
        "id",
        "store_id",
        "revenue",
        F.lead("units").over(w_hist).alias("next_units"),
        F.lead("revenue").over(w_hist).alias("next_revenue"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked_store_sales = (
    ranked
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select("id", "next_units", "units_share")
)

cleaned = (
    ranked_store_sales
    .filter(F.col("units_share") <= 0.53)
    .withColumn("next_units_times_units_share", (F.col("next_units") * F.col("units_share")))
    .withColumn("units_share_int", F.col("units_share").cast("int"))
)

kept_keys = (
    cleaned
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue").between(1900, 3300))
        .select("id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("units_share", "next_units_times_units_share", "units_share_int")
    .orderBy("units_share_int", "next_units_times_units_share")
)
