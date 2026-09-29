from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w_hist = Window.partitionBy("country").orderBy("age", "id")
w_grp = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("event_seq", "id")

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-22").cast("date")) & (F.col("country") != "JP"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "is_active",
        "age",
        F.lower("country").alias("country_lower"),
        F.concat("city", F.lit(" - "), "segment").alias("city_segment"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

with_windows = (
    base
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("age_bucket", F.ntile(4).over(w_hist))
    .withColumn("first_known_age", F.first("age", ignorenulls=True).over(w_grp))
    .withColumn("running_age", F.sum("age").over(w4))
    .withColumn("first_known_age_2", F.first("age", ignorenulls=True).over(w_grp))
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .select(
        "id",
        "name",
        "age",
        "city_segment",
        "age_bucket",
        "first_known_age",
        "running_age",
        "first_known_age_2",
    )
)
