from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.orderBy(F.col("rating_filled").desc(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city") == "Berlin")
    .withColumn("country_upper", F.upper("country"))
)

cleaned_suppliers = (
    cleaned
    .filter(F.col("city").isin(["Tokyo", "Berlin"]))
    .select(
        "id",
        "city",
        "rating",
        "country_upper",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.col("rating").cast("int").alias("rating_int"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

enriched = (
    cleaned_suppliers
    .withColumn("rating_bucket", F.ntile(5).over(w_ordered))
    .withColumn("rating_filled_bucket", F.ntile(4).over(w2))
)

result = (
    enriched
    .select(
        "id",
        "city",
        "rating",
        "country_upper",
        "rating_filled",
        "rating_int",
        "resolved_city",
        "rating_filled_bucket",
    )
    .orderBy(F.col("rating_int").desc())
)
