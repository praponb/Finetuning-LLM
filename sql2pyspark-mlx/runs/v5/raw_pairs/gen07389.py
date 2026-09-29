from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("country")
w_grp = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "DE"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.avg("rating").over(w).alias("rating_avg_2_prior"),
        F.sum("rating").over(w2).alias("country_sum_rating"),
        F.sum("rating").over(w_grp).alias("cumulative_rating"),
    )
)

result = (
    enriched
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "rating_avg_2_prior",
        "cumulative_rating",
        F.when(F.col("city").rlike("^(London|Chiang Mai)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|New York)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy("rating", "city_zone")
)
