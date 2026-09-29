from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("distinct_ids")
)
