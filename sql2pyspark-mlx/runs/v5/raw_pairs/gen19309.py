from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

result = (
    exploded
    .groupBy("code")
    .agg(
        F.size(F.collect_set("code")).alias("num_codes"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.sort_array(F.collect_set("code")).alias("code_list_2"),
    )
    .orderBy("code")
)
