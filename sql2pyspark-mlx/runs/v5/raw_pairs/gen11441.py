from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("num_rows_2"),
    )
    .orderBy(F.col("span_minutes").asc_nulls_last())
)
