from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_last(), "id")
w_running = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "supplier_name", "country", "city", "rating", "is_us")
)

scored = (
    latest
    .filter(F.col("is_us").between(56, 79))
    .select(
        "id",
        "city",
        "rating",
        "is_us",
        F.lead("is_us").over(w_running).alias("next_is_us"),
        F.lag("is_us").over(w_running).alias("prev_is_us"),
        F.lead("rating").over(w_running).alias("next_rating"),
        F.row_number().over(w_running).alias("event_seq"),
        F.last("is_us", ignorenulls=True).over(w3).alias("last_known_is_us"),
    )
)

cleaned = (
    scored
    .filter(F.col("city").isNotNull() & (F.col("rating") < 3.2))
    .withColumn("city_filled", F.coalesce("city", F.lit("unassigned")))
)

result = (
    cleaned
    .select("id", "rating", "is_us", "prev_is_us", "city_filled")
    .orderBy(F.col("id").asc_nulls_first())
)
