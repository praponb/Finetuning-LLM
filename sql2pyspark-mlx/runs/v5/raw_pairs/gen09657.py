from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("region", "city").orderBy("seq_no", "id")
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("region").orderBy(F.col("seq_no").desc_nulls_first())
w6 = Window.orderBy(F.col("next_sqft").asc_nulls_first())

valid = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-10-04").cast("date"))
    .withColumn("store_name_upper", F.upper("store_name"))
)

cleaned = (
    valid
    .filter(F.col("region").isNotNull())
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
)

scored = (
    cleaned
    .select(
        "*",
        F.lead("sqft").over(w).alias("next_sqft"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
    )
)

with_windows = (
    scored
    .withColumn("next_cumulative_sqft", F.lead("cumulative_sqft").over(w_ordered))
    .withColumn("next_sqft_2", F.lead("sqft").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)

kept_keys = (
    with_windows
    .select("city", "region")
    .subtract(
        df_stores
        .filter(F.col("opened_date") > "2024-05-18")
        .select("city", "region")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

with_windows_stores = (
    kept_rows
    .withColumn("seq_no_share", F.round((F.col("seq_no") / F.sum("seq_no").over(w4)), 4))
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w5))
    .withColumn("next_sqft_dense_rank", F.dense_rank().over(w6))
)

result = (
    with_windows_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "store_name_upper",
        "opened_date_label",
        "seq_no",
        F.when(F.col("store_name").rlike("^(Riverside|Siam Flagship)$"), "GROUP_A").when(F.col("store_name").rlike("^(Downtown Express|Airport Kiosk)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("store_name_zone"),
    )
    .orderBy(F.col("id").desc(), "opened_date")
)
