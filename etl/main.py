import sqlite3
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def extract():
    # Необходимо добавить документацию о том, какую именно информацию получаем из базы данных
    # Либо просто описать возвращаемые значения
    """
    extract data from sql-db
    :return:
    """

    # Хорошо выносить такие литералы в отдельный файл с переменными окружения или environment variables,
    # чтобы не дать обнаружить другим какую то чувствительную информацию вроде названий файлов, айпи, портов, паролей
    # Почитать как это делать:
    # https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1

    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()

    # Наверняка это пилится в один sql - запрос, но мне как-то лениво)

    # Все запросы, которые используются в скрипте также лучше помещать либо в отдельные файлы,
    # либо в переменные с прозрачным и ясным названием, например в данном случае
    # можно попробовать movie_enriched_with_actors_info или movie_and_actors_query
    # это придаст коду большую ясность и тот, кто его читает не будет отвлекаться на его содержимое,
    # а будет ориентироваться только на название или короткий комментарий

    # Также в данном случае можно было использовать JOIN и всего 1 подзапрос, работать будет чуточку быстрее

    # Получаем все поля для индекса, кроме списка актеров и сценаристов, для них только id
    cursor.execute("""
        select id, imdb_rating, genre, title, plot, director,
        -- comma-separated actor_id's
        (
            select GROUP_CONCAT(actor_id) from
            (
                select actor_id
                from movie_actors
                where movie_id = movies.id
            )
        ),

        max(writer, writers)
        from movies
    """)

    raw_data = cursor.fetchall()

    # Необходимо удалять отладочные принты после окончания работы над проектом

    # cursor.execute('pragma table_info(movies)')
    # pprint(cursor.fetchall())

    # Также неплохо выносить это все в переменные вроде actors_with_name, writers_with_name
    # Вообще говоря можно получение этих данных оформить в отдельные функции
    # А также добавить небольшую документацию

    # Нужны для соответсвия идентификатора и человекочитаемого названия
    actors = {row[0]: row[1] for row in cursor.execute('select * from actors where name != "N/A"')}
    writers = {row[0]: row[1] for row in cursor.execute('select * from writers where name != "N/A"')}

    return actors, writers, raw_data


# Неясно зачем добавлять к аргументам функции префикс в виде двойное подчеркивания
# Практика показывет, что обычно это используется для условного сокрытия переменных
# Внутри классов от пользователя этих классов и от потомков класса
# https://peps.python.org/pep-0008/#naming-conventions поискать фразу __double_leading_underscore
def transform(__actors, __writers, __raw_data):
    # Также необходимо описать то, каким именно образом данные трансформируются
    # Также описать аргументы функции и возвращаемое значение
    # В python есть type hints для облегчения понимания типов аргументов и возвращаемых значений
    # https://peps.python.org/pep-0484/
    """

    :param __actors:
    :param __writers:
    :param __raw_data:
    :return:
    """
    documents_list = []
    for movie_info in __raw_data:
        # Разыменование списка
        movie_id, imdb_rating, genre, title, description, director, raw_actors, raw_writers = movie_info

        # Довольно много логики здесь написано для получения списка авторов фильма
        # Хорошо было бы выделить ее в отдельную функцию с сигнатурой
        # def get_writers_list(raw_writers: str, writers_info: dict) -> list
        if raw_writers[0] == '[':
            parsed = json.loads(raw_writers)
            new_writers = ','.join([writer_row['id'] for writer_row in parsed])
        else:
            new_writers = raw_writers

        # Хотя здесь и есть небольшое дублирование, я думаю, что выделение общей части в функцию
        # Лишь уменьшит читаемость. В данном случае небольшое дублирование приемлемо
        writers_list = [(writer_id, __writers.get(writer_id)) for writer_id in new_writers.split(',')]
        actors_list = [(actor_id, __actors.get(int(actor_id))) for actor_id in raw_actors.split(',')]

        # Формирование словаря document выглядит очень громоздким
        # Некоторые значения формируются до объявления переменной document
        # Такие как writers_list и actors_list и продолжают формироваться
        # Внутри самого объявления через list comprehension
        # Некоторые значения по умолчанию сформированы и мы просто подставляем их
        # Как например movie_id, title
        # А есть значение genre, которое полностью формируется в объявлении переменной
        # Думаю было бы прозрачнее сделать полное формирование информации о документе
        # До его объявление и в объявлении просто подставить нужные значения
        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": imdb_rating,
            "genre": genre.split(', '),
            "title": title,
            "description": description,
            "director": director,
            "actors": [
                {
                    "id": actor[0],
                    "name": actor[1]
                }
                for actor in set(actors_list) if actor[1]
            ],
            "writers": [
                {
                    "id": writer[0],
                    "name": writer[1]
                }
                for writer in set(writers_list) if writer[1]
            ]
        }

        # Этот кусок кода можно также выделить в отдельную функцию вроде
        # def fill_na_document_values(document: dict) -> dict
        for key in document.keys():
            if document[key] == 'N/A':
                # Снова про отладочный print
                # print('hehe')
                document[key] = None

        # Почему то эта часть формирования документа вообще ушла куда то вниз
        # После заполнения N/A значений None значениями
        # Следует это все скомпоновать в одном месте
        document['actors_names'] = ", ".join([actor["name"] for actor in document['actors'] if actor]) or None
        document['writers_names'] = ", ".join([writer["name"] for writer in document['writers'] if writer]) or None

        # Хорошо было бы удалить отладочные вывод в консоль
        # Также хорошей практикой является объявление всех импортов в начале файла
        # https://peps.python.org/pep-0008/#imports
        # Так код выглядит более структурированным, а тот, кто его читает,
        # сразу будет понимать какие библиотеки используются
        import pprint
        pprint.pprint(document)

        documents_list.append(document)

    return documents_list


def load(acts):
    # Здесь также необхимо добавить документацию, но это не так критично
    # В целом понятно что она делает
    """

    :param acts:
    :return:
    """
    es = Elasticsearch([{'host': '192.168.1.252', 'port': 9200}])
    bulk(es, acts)

    # А вот тут непонятно. Почему возвращается True?
    # Вряд ли его кто-то ждет
    return True


if __name__ == '__main__':
    load(transform(*extract()))