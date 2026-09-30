from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region")
w2 = Window.orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("region", "city").orderBy(F.col("sqft").asc_nulls_first())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.orderBy("last_restock", "id")
w_part = Window.orderBy(F.col("seq_no").desc_nulls_first())
w_part_2 = Window.orderBy(F.col("next_sqft_share").desc_nulls_last())

base = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.lower("region").alias("region_lower"),
    )
)

enriched = (
    base
    .select(
        "id",
        "region_lower",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.avg("sqft").over(w2).alias("sqft_avg_5"),
        F.dense_rank().over(w3).alias("sqft_dense_rank"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"), F.col("c.quantity"))
)

scored = (
    with_last
    .filter(F.col("quantity") != 131)
    .select(
        "id",
        "sqft_avg_5",
        "last_restock",
        F.lag("sqft_avg_5").over(w5).alias("prev_sqft_avg_5"),
        F.lead("sqft_share").over(w5).alias("next_sqft_share"),
        F.lag("sqft_share").over(w5).alias("prev_sqft_share"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

remaining_keys = (
    scored
    .select("id")
    .intersect(
        df_stores
        .filter(F.col("store_name").like("M%"))
        .select("id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "id",
        "prev_sqft_avg_5",
        "seq_no",
        F.percent_rank().over(w_part).alias("seq_no_percent_rank"),
        F.dense_rank().over(w_part_2).alias("next_sqft_share_dense_rank"),
    )
)

result = metrics.select("prev_sqft_avg_5", "seq_no", "seq_no_percent_rank")
