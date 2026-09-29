from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
    )
)
