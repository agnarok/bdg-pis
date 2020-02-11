import time
from pyspark import SparkConf, SparkContext
from heapq import nsmallest, nlargest


def parser(array):  # "God is dead. And we have killed him" - Friedrich Nietzsche
    obj = {}
    obj[array[0][0].strip()] = int(array[0][1].strip())  # Id
    obj[array[1][0].strip()] = array[1][1].strip()  # ASIN

    title = ''
    for til in array[2][1:-2]:
        title += til
    title += array[2][-1]
    obj[array[2][0].strip()] = title  # title

    obj[array[3][0].strip()] = array[3][1].strip()  # group
    obj[array[4][0].strip()] = int(array[4][1].strip())  # salesrank

    obj[array[5][0].strip()] = array[5][1][2:].split()  # similars

    n_categories = int(array[6][1].strip())
    obj[array[6][0].strip()] = []  # categories
    for i in range(0, n_categories):
        obj[array[6][0].strip()].append(array[i+7][0].strip().split('|')[-1])

    skip = 8+n_categories
    n_reviews = int(array[7+n_categories][2].split()[0])
    obj['reviews'] = []  # reviews
    for i in range(0, n_reviews):
        obj['reviews'].append({
            'date': array[skip+i][0].split()[0],
            'customer': array[skip+i][1].split()[0],
            'rating': int(array[skip+i][2].split()[0]),
            'votes': int(array[skip+i][3].split()[0]),
            'helpful': int(array[skip+i][4].split()[0])
        })

    return obj


def get_context(app):
    conf = SparkConf().setAppName(app).setMaster('local')
    return SparkContext(conf=conf)


def mean_rating_reviews(product):
    mean = 0
    for r in product['reviews']:
        mean += r['rating']
    mean = mean/len(product['ASIN'])
    return mean


def mean_helpful_reviews(product):
    mean = 0
    for r in product['reviews']:
        mean += r['helpful']
    mean = mean/len(product['ASIN'])
    return mean


def prepare_data():
    conf = SparkConf().setAppName('T1').setMaster('local')
    sc = SparkContext(conf=conf)
    sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")
    t_file = sc.textFile('sample.txt')
    lines = t_file.filter(lambda line: not (
        'discontinued' in line or line.startswith('#')))
    arrays = lines.map(lambda line: line.splitlines())
    pre_objects = arrays.map(lambda obj: [key.split(':') for key in obj])
    products = pre_objects.map(parser)
    return products, sc  # Retorna um RDD base e o contexto do spark para uso auxiliar


def most_helpful_reviews(products, sc):  # letra A
    reviews = products.map(lambda product: (product['ASIN'], sorted(
        product['reviews'], key=lambda p: p['helpful'], reverse=True)[0:10]))
    reviews.saveAsTextFile('a')


def top_similars(products, sc):  # letra B
    similars = products.map(lambda product: (
        product['ASIN'], product['similar']))
    similars.saveAsTextFile('b')


def reviews_evolution(products, sc):  # letra C
    reviews = products.map(lambda product: {
        'product': product['ASIN'],
        'reviews': sorted(product['reviews'], key=lambda r: time.strptime(r['date'], '%Y-%m-%d'))
    })
    reviews.saveAsTextFile('c')


def top_product_group(products, sc):  # letra D
    groups = products.map(lambda product: (
        product['group'], product['salesrank']))
    result = groups.groupByKey().map(
        lambda p: (p[0], nsmallest(10, p[1])))
    result.saveAsTextFile('d')


def top_products_reviewd(products, sc):  # letra E
    top = products.map(lambda p: (p['ASIN'], mean_rating_reviews(p)))
    top.saveAsTextFile('e')


def top_rating_categories(products, sc):  # letra F
    top = products.map(lambda p: (
        p['categories'], mean_helpful_reviews(p)))
    top.saveAsTextFile('f')


def users_with_most_review(products, sc):  # letra G
    spark = sc
    products = products.flatMap(lambda p: p['reviews']).collect()
    reviews = spark.parallelize(products).map(lambda r: (r['customer'], 1))
    result = reviews.reduceByKey(lambda a, b: a+b)
    result.saveAsTextFile('g')


if __name__ == "__main__":
    products, sc = prepare_data()
    most_helpful_reviews(products, sc)  # A
    top_similars(products, sc)  # B
    reviews_evolution(products, sc)  # C
    top_product_group(products, sc)  # D
    top_products_reviewd(products, sc)  # E
    top_rating_categories(products, sc)  # F
    users_with_most_review(products, sc)  # G
