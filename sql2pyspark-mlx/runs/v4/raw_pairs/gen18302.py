from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("rating", "id").rowsBetween(-2, -1)

scored = (
    df_suppliers
    .select(
        "*",
        F.sum("rating").over(w_part).alias("running_rating"),
        F.avg("rating").over(w2).alias("rating_avg_2_prior"),
    )
)

result = (
    scored
    .select(
        "id",
        "country",
        "city",
        "rating",
        "running_rating",
        F.when(F.col("country").rlike("^(US|JP)$"), "GROUP_A").when(F.col("country").rlike("^(TH|SG)$"), "PRIORITY").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy("running_rating", F.col("id").desc())
)
