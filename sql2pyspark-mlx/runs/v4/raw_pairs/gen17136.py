from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("distinct_countries").desc())

filtered = (
    df_suppliers
    .filter(F.col("supplier_name").like("Sia%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

result = (
    filtered
    .groupBy("city", "country")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
    )
    .withColumn("distinct_countries_dense_rank", F.dense_rank().over(w))
)
