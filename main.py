from pyspark.sql import SparkSession
import os

csv_path = os.path.join(os.path.dirname(__file__), "nuek-vuh3.csv")

try:

    # Створюємо сесію Spark
    spark = ((((SparkSession.builder
            .master("local[*]"))
            .config("spark.sql.shuffle.partitions", "2"))
            .appName("MyGoitSparkSandbox"))
            .getOrCreate())

    # Завантажуємо датасет
    nuek_df = (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(csv_path))

    nuek_repart = nuek_df.repartition(2)



    nuek_processed = (nuek_repart
                    .where("final_priority < 3")
                    .select("unit_id", "final_priority")
                    .groupBy("unit_id").count().cache())

    nuek_processed.collect()

    nuek_processed = nuek_processed.where("count>2")

    result = nuek_processed.collect()
    print(result)
    input("Press Enter to continue...")

finally:
    # Закриваємо сесію Spark
    spark.stop()
