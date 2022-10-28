from flask import Flask, abort, request, jsonify
import elasticsearch as ES

from validate import validate_args

app = Flask(__name__)

# Различные подключения к базам данным и иным сервисам
# Лучше осуществлять в начале файла, а еще лучше создать
# Директорию с файлами всех необходимых коннектов и импортировать
# Их в главный файл Flask-приложения

# Например:
# from connections.elasticsearch import es
# es.connect([{"host": <host>, "port": <port>}], )


# Если эта функция используется для проверки работоспособности сервера
# Возможно стоит сделать
# @app.route('/ping')
# def ping():

@app.route('/')
def index():
    return 'worked'


@app.route('/api/movies/')
def movie_list():
    validate = validate_args(request.args)

    if not validate['success']:
        return abort(422)

    # Думаю что заполнение словаря дефолтными параметрами
    # Нужно вынести в отдельную функцию
    # def get_default_params(request: "Request") -> tp.Dict

    defaults = {
        'limit': 50,
        'page': 1,
        'sort': 'id',
        'sort_order': 'asc'
    }

    # Тут уже валидно все
    for param in request.args.keys():
        defaults[param] = request.args.get(param)

    # Также нужно вынести в отдельную функцию формирование body словаря
    # Для запроса в Elasticsearch. Это придаст коду ясность и простоту
    # Тому кто читает код нужно будет только понимать ЧТО именно происходит
    # А не то, КАК это происходит. Это упрощает общее понимание программы

    # Уходит в тело запроса. Если запрос не пустой - мультисерч, если пустой - выдает все фильмы
    body = {
        "query": {
            "multi_match": {
                "query": defaults['search'],
                "fields": ["title"]
            }
        }
    } if defaults.get('search', False) else {}

    body['_source'] = dict()
    body['_source']['include'] = ['id', 'title', 'imdb_rating']

    # То же самое можно сказать и про этот кусочек кода. Вынести в отдельную функцию

    params = {
        # Необходимо удалять закомментированный код после его отладки
        # Чтобы не вводить других программистов и себя из будущего
        # В заблуждение)
        # '_source': ['id', 'title', 'imdb_rating'],
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }

    # Каждый запрос открывает и закрывает подключение к ElasticSearch
    # Это забирает на себя ресурсы и время во время обработки запроса
    # Лучше вынести подключение в начало файла
    # Так же по PEP следует избегать лишних пробелов в круглых скобках
    # https://peps.python.org/pep-0008/#whitespace-in-expressions-and-statements
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )

    # Думаю что здесь тоже можно вынести код формирование итогового json объекта
    # В отдельную функцию и написать к ней небольшую документацию, так как здесь
    # Используется переменная search_res, а сам ответ лежит под двумя ключами ['hits']['hits']
    # Если не залезть в документацию к Elasticsearch, то будет неясно почему ответ лежит именно там

    # По итогу функция movie_list может быть написана следующим образом:
    # validate params
    # default_params = get_default_params
    # body = get_es_query_body
    # params = get_es_query_params
    # return get_movies_query

    search_res = es_client.search(
        body=body,
        index='movies',
        params=params,
        filter_path=['hits.hits._source']
    )
    es_client.close()

    return jsonify([doc['_source'] for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )

    # Здесь проверяется доступность ES кластера, но в случае
    # Его недоступности просто выводится отладочный print
    # Думаю неплохо было бы сделать обработку такой ситуации
    # Более осмысленной и отправлять 500 ошибку пользователю
    # Или сообщение о временной недоступности сервиса
    if not es_client.ping():
        print('oh(')

    # Весь этот код кроме строки es_client.close() можно поместить
    # В функцию get_query_movie и написать return get_query_movie
    search_result = es_client.get(index='movies', id=movie_id, ignore=404)

    es_client.close()

    if search_result['found']:
        return jsonify(search_result['_source'])

    return abort(404)


# Здесь было оформление кода не по PEP:
# Между блоками кода должно быть две строки отступа
# https://peps.python.org/pep-0008/#blank-lines
# А последняя строка кода должна быть пустой
#
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
