from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy("store_id", "city").orderBy("opened_date", "id")
w4 = Window.partitionBy("city")
w5 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_region", "region").orderBy("next_sqft", "id")
w7 = Window.partitionBy("city", "region").orderBy("seq_no_share", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("region").orderBy("sqft", "id")

with_dims = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.opened_date"), F.col("b.region"), F.col("b.store_name"))
)

deduped = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

joined = (
    deduped
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.opened_date") >= "2023-09-04")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.opened_date",
        "src.region",
        F.col("d.region").alias("store_region"),
        F.col("d.city"),
        F.col("d.sqft"),
    )
)

scored = (
    joined
    .select(
        "id",
        "region",
        "store_region",
        "city",
        "sqft",
        F.lag("sqft").over(w2).alias("prev_sqft"),
        F.lead("sqft").over(w2).alias("next_sqft"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "region",
        "city",
        "sqft",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w4)), 4).alias("seq_no_share"),
        F.first("region", ignorenulls=True).over(w5).alias("first_known_region"),
        F.lag("prev_sqft", 2).over(w6).alias("prev_prev_sqft"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "region",
        "city",
        "sqft",
        "first_known_region",
        F.sum("sqft").over(w7).alias("last3_sum_sqft"),
        F.max("sqft").over(w7).alias("last3_max_sqft"),
        F.avg("sqft").over(w7).alias("last3_avg_sqft"),
        F.min("sqft").over(w7).alias("last3_min_sqft"),
        F.lead("prev_prev_sqft", 2, 0).over(w8).alias("next_prev_prev_sqft"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock").between("2023-08-01", "2025-10-01"))
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("region")
    .pivot("city", ["Bangkok", "Berlin", "Tokyo"])
    .agg(F.sum("last3_max_sqft"))
)
