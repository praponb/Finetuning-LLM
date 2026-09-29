from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating_filled").desc())

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("P%"))
    .select("*", F.coalesce("rating", F.lit(0)).alias("rating_filled"))
)

cleaned = (
    base
    .filter(F.col("city").isNotNull())
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 2))
)

filtered = (
    cleaned
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "city",
        "rating_filled",
        "supplier_name_prefix",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    ranked
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .withColumn("rating_filled_band", F.when(F.col("rating_filled") >= 68, "High").when(F.col("rating_filled") >= 23, "Medium").otherwise("Low"))
)

result = (
    valid
    .select(
        "id",
        "city",
        "rating_filled",
        "supplier_name_prefix",
        "supplier_name_normalized",
        "rating_filled_band",
    )
    .orderBy(F.col("rating_filled").asc_nulls_last())
)
