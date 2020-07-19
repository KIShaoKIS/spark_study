from src.utils.common import get_spark_session

ss = get_spark_session()
sc = ss.sparkContext
text = sc.textFile('SayingGoodByeToCambridgeAgain.txt')
words = text.flatMap(lambda line: line.split(" "))
word_cnt = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a +b)
word_cnt.saveAsTextFile('word_count.output')

