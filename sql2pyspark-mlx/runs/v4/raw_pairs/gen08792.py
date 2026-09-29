from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

deduped = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("quantity") >= 7, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
        "order_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("quantity_tier").desc_nulls_last())
)
