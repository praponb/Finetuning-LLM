from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.date_trunc("QUARTER", "sale_date").alias("quarter_start"))
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.bit_or("units").alias("units_bit_or"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("units").alias("units_bit_and_2"),
    )
)
