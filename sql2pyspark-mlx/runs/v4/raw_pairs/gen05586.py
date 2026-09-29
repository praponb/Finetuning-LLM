from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("element").orderBy(F.col("element_length").desc_nulls_last())
w2 = Window.partitionBy("element")
w3 = Window.partitionBy("element").orderBy(F.col("element_length").desc_nulls_first())

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

lines = flattened.select("id", "value", F.explode("coupon_codes").alias("element"))

cleaned = (
    lines
    .filter(F.col("element").isNotNull() & (F.col("element") != "VIP"))
    .withColumn("clean_element", F.trim("element"))
    .withColumn("element_length", F.length("element"))
    .withColumn("element_filled", F.coalesce("element", F.lit("NONE")))
)

enriched = (
    cleaned
    .withColumn("element_length_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("element_max_element_length", F.max("element_length").over(w2))
    .withColumn("element_length_cume_dist_2", F.cume_dist().over(w3))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("element_length_cume_dist") >= 0.65, "LARGE").otherwise("SMALL").alias("element_length_cume_dist_tier"),
        "element",
    )
    .agg(
        F.size(F.collect_set("element")).alias("num_elements"),
        F.round(F.sum("element_length") / F.nullif(F.count(F.when(F.col("element") == "WELCOME5", 1)), F.lit(0)), 2).alias("element_length_per_welcome5"),
    )
    .orderBy("element")
)
