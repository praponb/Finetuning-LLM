from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy(F.col("created_at").desc(), "id", "line_no")
w3 = Window.partitionBy("customer_id", "promo").orderBy("product_id", "id", "line_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "promo").orderBy("customer_id_max_qty", "id", "line_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("product_id", "id", "line_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("coupon_codes_count", "id", "line_no")

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("line_no"),
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.promo"),
    )
)

scored = (
    exploded
    .filter(F.col("qty") < 3)
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w)), 4))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("customer_id_max_qty", F.max("qty").over(w))
    .withColumn("running_product_id", F.sum("product_id").over(w3))
)

cleaned = (
    scored
    .filter(F.col("line_no") > 3)
    .select(
        "*",
        F.col("product_id_share").cast("int").alias("product_id_share_int"),
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.coalesce("promo", F.lit("DIRECT")).alias("resolved_promo"),
    )
)

scored_carts = (
    cleaned
    .withColumn("running_row_seq", F.sum("row_seq").over(w4))
    .withColumn("last_known_promo", F.last("promo", ignorenulls=True).over(w5))
    .withColumn("prev_customer_id_max_qty", F.lag("customer_id_max_qty", 2, 0).over(w6))
)

result = (
    scored_carts
    .select(
        "id",
        "coupon_codes",
        "line_no",
        "product_id",
        "promo",
        "product_id_share",
        "customer_id_max_qty",
        "running_product_id",
        "product_id_share_int",
        F.when(F.col("promo").rlike("^(WELCOME5|SPRING10)$"), "CORE").when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("GROUP_A").alias("promo_zone"),
    )
    .orderBy(F.col("promo").desc_nulls_first())
)
