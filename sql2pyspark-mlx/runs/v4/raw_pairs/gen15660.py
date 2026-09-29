from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_suppliers
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("rating_share_2", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("rating_avg_5_prior", F.avg("rating").over(w2))
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w3))
)

result = (
    metrics
    .select(
        "id",
        "supplier_name",
        "country",
        "rating_share",
        "rating_share_2",
        "rating_avg_5_prior",
        "first_known_rating",
        F.when(F.col("country").rlike("^(UK|TH)$"), "CORE").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("rating_share").desc_nulls_last())
)
