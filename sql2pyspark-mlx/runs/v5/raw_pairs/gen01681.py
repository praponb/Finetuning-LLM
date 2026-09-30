from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") > "2024-02-17") & (F.col("category") == "Books") & (F.col("cost") < 460))
    .select(
        "id",
        "tags",
        "cost",
        F.when(F.col("brand").isin(["Globex", "Acme"]), "Tier 1").otherwise("Other").alias("brand_group"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)
