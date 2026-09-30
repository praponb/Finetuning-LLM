from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc())

enriched = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_6_prior"),
        F.percent_rank().over(w2).alias("rating_percent_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rating_percent_rank",
        F.when(F.col("city").rlike("^(New York|Berlin)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
