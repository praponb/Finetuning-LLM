from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("brand", "category").orderBy("cost", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("brand", "category").orderBy(F.col("cost_rounded").desc_nulls_last())

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-05-29").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        F.round("cost", 2).alias("cost_rounded"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

result = (
    valid
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.avg("cost_rounded").over(w2).alias("cost_rounded_avg_5_prior"),
        F.rank().over(w3).alias("cost_rounded_rank"),
    )
)
