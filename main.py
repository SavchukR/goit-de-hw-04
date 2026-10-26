from pyspark.sql import SparkSession
import os

csv_path = os.path.join(os.path.dirname(__file__), "nuek-vuh3.csv")

try:

    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .appName("MyGoitSparkSandbox") \
        .getOrCreate()

    # Завантажуємо датасет
    nuek_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)

 
    nuek_repart = nuek_df.repartition(2)

    nuek_processed_cached = nuek_repart \
        .where("final_priority < 3") \
        .select("unit_id", "final_priority") \
        .groupBy("unit_id") \
        .count() \
        .cache()  # Додано функцію cache

    # Проміжний action: collect
    nuek_processed_cached.collect()

    # Ось ТУТ додано рядок
    nuek_processed = nuek_processed_cached.where("count>2")

    nuek_processed.collect()

    input("Press Enter to continue...5")

    # Звільняємо пям'ять від Dataframe
    nuek_processed_cached.unpersist()


finally:
    # Закриваємо сесію Spark
    spark.stop()
