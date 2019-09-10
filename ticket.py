import csv
import re
from pymongo import MongoClient

client = MongoClient()
tickbd = client['tickbd']


def load_tickets(bd, collection='ticket', csv_file='ticket.csv'):
    """
    Загрузка данных в коллекцию ticket из CSV-файла
    """
    concert_ids_list = load_concerts(tickbd, 'concert', 'concert.csv')

    with open(csv_file, encoding='utf8') as csvfile:
        data_list = []
        reader = csv.DictReader(csvfile)
        for row in reader:
            data = dict(row)
            data['concert_id'] = concert_ids_list[int(data['concert_id'])-1]
            # print(data)
            data_list.append(data)
    ticket_list = bd[collection].insert_many(data_list)
    return ticket_list.inserted_ids


def load_concerts(bd, collection='concert', csv_file='concert.csv'):
    """
    Загрузка данных в коллекцию concert из CSV-файла
    """
    artist_ids_list = read_data(tickbd, 'artist', 'artist.csv')
    location_ids_list = read_data(tickbd, 'location', 'location.csv')
    town_ids_list = read_data(tickbd, 'town', 'town.csv')
    country_ids_list = read_data(tickbd, 'country', 'country.csv')

    with open(csv_file, encoding='utf8') as csvfile:
        data_list = []
        reader = csv.DictReader(csvfile)
        for row in reader:
            data = dict(row)
            data['artist_id'] = artist_ids_list[int(data['artist_id'])-1]
            data['location_id'] = location_ids_list[int(data['location_id'])-1]
            data['town_id'] = town_ids_list[int(data['town_id'])-1]
            data['country_id'] = country_ids_list[int(data['country_id'])-1]
            # print(data)
            data_list.append(data)
    concert_list = bd[collection].insert_many(data_list)
    return concert_list.inserted_ids


def read_data(bd, collection, csv_file):
    """
    Загрузка данных в остальные коллекции из CSV-файлов
    """
    with open(csv_file, encoding='utf8') as csvfile:
        data_list = []
        reader = csv.DictReader(csvfile)
        for row in reader:
            data = dict(row)
            # print(data)
            data_list.append(data)
    row_list = bd[collection].insert_many(data_list)
    return row_list.inserted_ids


def find_cheapest(db):
    """
    Сортировка билетов по возрастания цены
    """
    return list(db.ticket.find().sort("price"))


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке),
    и вернуть их по возрастанию цены
    """
    # !!! ищет только исполнителя по регулярке, без join'а с коллекцией ticket:

    str = ".*({}).*".format(name)
    regex = re.compile(str, re.IGNORECASE)
    return list(db.artist.find({"title_artist": {"$regex": regex}}))


if __name__ == '__main__':

    # загружаются билеты, концерты, места, города и страны,
    # из разных csv по разным коллекциям с учетом _id:
    # load_tickets(tickbd, 'ticket', 'ticket.csv')

    # а как объединять(join'ить) коллекции по _id - не разобралась:
    print(find_cheapest(tickbd))

    print(find_by_name('cri', tickbd))



