from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("is_singapore", "id")
w_hist = Window.partitionBy("city").orderBy("is_singapore", "id")
w_recent = Window.partitionBy("city").orderBy("prev_is_singapore_2", "id")
w_part = Window.orderBy("prev_prev_is_singapore", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("prev_is_singapore_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.lag("is_singapore", 2).over(w).alias("prev_is_singapore"),
        F.lag("is_singapore", 2).over(w_hist).alias("prev_is_singapore_2"),
    )
)

metrics_suppliers = (
    metrics
    .withColumn("next_prev_is_singapore", F.lead("prev_is_singapore").over(w_recent))
    .withColumn("prev_prev_is_singapore", F.lag("prev_is_singapore").over(w_recent))
    .withColumn("prev_is_singapore_3", F.lag("is_singapore").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent))
)

with_windows = (
    metrics_suppliers
    .filter(F.col("prev_is_singapore_3") == 34)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "is_singapore",
        "city_filled",
        "next_prev_is_singapore",
        "prev_prev_is_singapore",
        "event_seq",
        F.last("prev_prev_is_singapore", ignorenulls=True).over(w_part).alias("last_known_prev_prev_is_singapore"),
        F.sum("prev_is_singapore_2").over(w5).alias("cumulative_prev_is_singapore_2"),
    )
)

result = (
    with_windows
    .groupBy("country")
    .pivot("city", ["Tokyo", "Singapore"])
    .agg(F.sum("prev_prev_is_singapore"))
    .select(
        "country",
        F.col("Tokyo").alias("tokyo"),
        F.col("Singapore").alias("singapore"),
    )
)
