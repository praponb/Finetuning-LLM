from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")
w2 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("promo_code_length", "id")
w4 = Window.orderBy("starts_on", "id")
w5 = Window.orderBy("ends_on", "id")
w6 = Window.orderBy(F.col("promo_code_length").asc_nulls_first())

valid = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-09-29").cast("date")) & (F.year("ends_on") == 2025))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.length("promo_code").alias("promo_code_length"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "starts_on", "ends_on", "promo_code_length")
)

base = (
    deduped
    .filter(F.col("starts_on") > F.lit("2023-07-29").cast("date"))
    .select(
        "*",
        F.date_format("ends_on", "yyyy-MM-dd").alias("ends_on_label"),
        F.date_format("starts_on", "yyyy-MM-dd").alias("starts_on_label"),
    )
)

with_windows = (
    base
    .filter(F.col("starts_on").between("2024-08-20", "2025-01-01"))
    .withColumn("running_promo_code_length", F.sum("promo_code_length").over(w2))
    .withColumn("promo_code_length_bucket", F.ntile(4).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("prev_promo_code_length", F.lag("promo_code_length", 1).over(w5))
    .withColumn("promo_code_length_cume_dist", F.cume_dist().over(w6))
)

filtered = (
    with_windows
    .filter(F.col("starts_on") > F.to_date(F.lit("2023-08-07")))
    .select(
        "*",
        (F.col("promo_code_length_bucket") * F.col("promo_code_length")).alias("promo_code_length_bucket_times_promo_code_length"),
    )
)

result = (
    filtered
    .select(
        "id",
        "starts_on",
        "ends_on",
        "promo_code_length",
        "ends_on_label",
        "row_seq",
        "prev_promo_code_length",
        "promo_code_length_cume_dist",
        "promo_code_length_bucket_times_promo_code_length",
    )
    .orderBy(F.col("ends_on").desc_nulls_first())
)
