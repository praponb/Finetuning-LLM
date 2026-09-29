from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "*",
        F.lead("rating", 2, 0).over(w).alias("next_rating"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("running_rating") >= 15, "PREMIUM").otherwise("STANDARD").alias("running_rating_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
    )
)
