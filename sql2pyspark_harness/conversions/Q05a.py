"""Q05a — Higher-order functions on arrays / maps / structs (reference conversion).

Note: the SQL defines a prod_attr CTE that the final SELECT never uses, so it is
dropped here — a converter should either do the same or keep it harmlessly unused.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build(spark: SparkSession) -> DataFrame:
    c, o = spark.table("customers").alias("c"), spark.table("orders").alias("o")

    cust_profile = (
        c.join(o, F.col("o.customer_id") == F.col("c.customer_id"), "left")
         .groupBy("c.customer_id", "c.full_name", "c.interests", "c.preferences")
         .agg(
             F.collect_list(F.struct(
                 F.col("o.order_id").alias("order_id"),
                 F.col("o.order_ts").alias("ts"),
                 F.col("o.status").alias("status"),
                 F.col("o.ship_to.city").alias("city"),
             )).alias("order_structs")
         )
    )

    interests, prefs, orders_ = F.col("interests"), F.col("preferences"), F.col("order_structs")
    real_orders = F.filter(orders_, lambda s: s.order_id.isNotNull())

    return (
        cust_profile.select(
            "customer_id",
            "full_name",
            F.size(interests).alias("n_interests"),
            F.transform(interests, lambda x: F.initcap(x)).alias("interests_title"),
            F.filter(interests, lambda x: x.isin("electronics", "books")).alias("tech_or_books"),
            F.exists(interests, lambda x: x == "beauty").alias("likes_beauty"),
            F.forall(orders_, lambda s: s.status.isin("DELIVERED", "SHIPPED") | s.order_id.isNull())
             .alias("all_orders_ok"),
            F.aggregate(F.transform(real_orders, lambda s: F.lit(1)), F.lit(0), lambda acc, x: acc + x)
             .alias("order_count_hof"),
            # F.get() returns NULL when out of bounds; [0] would throw under ANSI mode
            F.get(
                F.array_sort(
                    real_orders,
                    lambda l, r: F.when(l.ts > r.ts, -1).when(l.ts < r.ts, 1).otherwise(0),
                ),
                0,
            )["order_id"].alias("latest_order_id"),
            F.array_distinct(
                F.transform(F.filter(orders_, lambda s: s.city.isNotNull()), lambda s: s.city)
            ).alias("ship_cities"),
            F.map_filter(prefs, lambda k, v: v == "Y").alias("opted_in_channels"),
            F.transform_values(prefs, lambda k, v: v == "Y").alias("prefs_bool"),
            F.map_keys(prefs).alias("pref_keys"),
            F.element_at(prefs, F.lit("newsletter")).alias("newsletter_flag"),
            F.zip_with(
                interests,
                F.sequence(F.lit(1), F.size(interests)),
                lambda i, n: F.concat(n.cast("string"), F.lit(":"), i),
            ).alias("ranked_interests"),
        )
        .orderBy("customer_id")
    )
