from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w_part = Window.partitionBy("country", "city")

scored = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.avg("rating").over(w_running).alias("rating_avg_5"),
        F.count("rating").over(w_part).alias("country_count_rating"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("country_count_rating") >= 16, "HIGH").otherwise("LOW").alias("country_count_rating_tier"),
        "city",
    )
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.max("country_count_rating").alias("max_country_count_rating"),
    )
)
