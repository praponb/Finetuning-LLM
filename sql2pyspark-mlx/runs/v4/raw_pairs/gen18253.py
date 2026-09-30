from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "ship_to",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.col("attributes").getItem("gift").alias("gift"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
    )
)

result = (
    unpacked
    .rollup("customer_id")
    .agg(
        F.percentile_approx("num_attributes", [0.25, 0.5, 0.75]).alias("num_attributes_quartiles"),
        F.countDistinct("gift").alias("distinct_gifts"),
        F.round(F.avg("num_attributes"), 2).alias("avg_num_attributes"),
        F.count("*").alias("row_count"),
        F.avg("num_attributes").alias("avg_num_attributes_2"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("distinct_gifts") < 1)
    .orderBy("row_count")
)
