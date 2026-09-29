from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")
w3 = Window.partitionBy("country").orderBy("signup_date", "id")
w4 = Window.partitionBy("country", "segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy(F.col("signup_date").asc_nulls_last(), "id")

valid = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2024-02-22")))
    .select(
        "*",
        F.concat("name", F.lit(" "), "country").alias("name_country"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

base = (
    valid
    .filter(F.col("segment").isNotNull())
    .select("*", F.concat_ws(",", "name", "segment").alias("name_segment_key"))
)

with_windows = (
    base
    .select(
        "*",
        F.stddev_samp("credit_limit").over(w).alias("segment_stddev_samp_credit_limit"),
        F.lead("age", 1).over(w2).alias("next_age"),
        F.lag("age", 2, 0).over(w3).alias("prev_age"),
        F.first("credit_limit", ignorenulls=True).over(w4).alias("first_known_credit_limit"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
    )
)
