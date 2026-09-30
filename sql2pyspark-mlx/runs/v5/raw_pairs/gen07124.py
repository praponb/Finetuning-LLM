from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "created_at",
        "pos",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.sum(F.when(F.col("promo") == "BUNDLE", F.col("price")).otherwise(0)).alias("bundle_price"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.sum(F.when(F.col("promo") == "SPRING10", F.col("price")).otherwise(0)).alias("spring10_price"),
    )
    .filter(F.col("spring10_price") < 1000000)
    .orderBy("bundle_price")
)
