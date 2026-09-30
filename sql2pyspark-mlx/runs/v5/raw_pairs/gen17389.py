from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.orderBy(F.col("ends_on").desc(), "id")
w3 = Window.orderBy("ends_on", "id")

base = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-08-03").cast("date")) & ~F.col("promo_code").isin(["BUNDLE", "SPRING10", "WELCOME5"]))
    .withColumn("ends_on_period", F.date_trunc("month", "ends_on"))
    .withColumn("discount_pct_level", F.when(F.col("discount_pct") >= 0.31, "High").when(F.col("discount_pct") >= 0.1, "Medium").otherwise("Low"))
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "ends_on")
)

with_windows = (
    ranked
    .select(
        "id",
        "ends_on",
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

base_promotions = (
    with_windows
    .filter(F.col("ends_on") >= F.lit("2023-10-13").cast("date"))
    .select(
        "*",
        F.least("seq_no", "row_seq").alias("least_seq_no_row_seq"),
        F.col("event_seq").cast("double").alias("event_seq_num"),
    )
)

result = (
    base_promotions
    .groupBy(
        F.date_trunc("WEEK", "ends_on").alias("week_start"),
        F.date_trunc("QUARTER", "ends_on").alias("quarter_start"),
        "seq_no",
    )
    .agg(
        F.round(F.avg("event_seq_num"), 2).alias("avg_event_seq_num"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("least_seq_no_row_seq"), 2).alias("avg_least_seq_no_row_seq"),
        F.round(F.avg("event_seq_num"), 2).alias("avg_event_seq_num_2"),
    )
)
