from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("is_jp", "id")

cleaned = (
    df_suppliers
    .filter(F.col("city") == "Bangkok")
    .withColumn("country_upper", F.upper("country"))
)

base = (
    cleaned
    .filter(~F.col("city").eqNullSafe(F.lit("Tokyo")))
    .withColumn("is_jp", F.when(F.col("country") == "JP", 1).otherwise(0))
)

valid = (
    base
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "country_upper",
        "is_jp",
        (F.col("rating") / F.nullif("is_jp", F.lit(0))).alias("rating_per_is_jp"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .select(
        "id",
        "supplier_name",
        "country",
        "is_jp",
        "rating_per_is_jp",
        "resolved_city",
    )
    .orderBy(F.col("rating_per_is_jp").asc_nulls_last())
)
