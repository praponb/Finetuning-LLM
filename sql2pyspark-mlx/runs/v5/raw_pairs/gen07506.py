from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country").orderBy("rating", "id")

metrics = (
    df_suppliers
    .filter(F.col("rating") > 4.0)
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.lead("rating", 1, 0).over(w2).alias("next_rating"),
    )
)

result = (
    metrics
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("us_rows").asc_nulls_first())
)
