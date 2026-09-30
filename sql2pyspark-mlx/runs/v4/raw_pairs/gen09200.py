from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country")
w2 = Window.partitionBy("city", "country").orderBy("rating", "id")
w_recent = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_last())
w4 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNotNull()), "left_anti")
)

metrics = (
    eligible
    .select(
        "*",
        F.max("rating").over(w_part).alias("country_max_rating"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
        F.rank().over(w_recent).alias("rating_rank"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "prev_rating",
        "rating_rank",
        "rank_in_group",
    )
    .orderBy(F.col("rank_in_group").desc_nulls_last())
)
