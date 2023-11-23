import csv
import requests
import time
import schedule
from pathlib import Path
import telebot
from datetime import datetime as dt
import os
from to_open import check_to_repeat, get_pair_name
from dotenv import load_dotenv

load_dotenv()
TELEGRAM_TOKEN = os.getenv('token')
TELEGRAM_CHAT_ID = os.getenv('chat_id')


# todo: get more functionality
def get_public_symbols():
    url = "https://api.bybit.com/spot/v3/public/symbols"
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    coins = data['result']['list']
    for coin in coins:
        coins_all_to_usdt = coin['name']
        if coins_all_to_usdt.endswith('USDT'):
            public_symbols.append(coins_all_to_usdt)
    return public_symbols


def time_start(year: int, month: int, day: int):
    t = (year, month, day, 4, 00, 00, 5, 341, 0)
    start_time = round(time.mktime(t) * 1000)
    return start_time


def time_end(year: int, month: int, day: int):
    t = (year, month, day, 4, 00, 00, 5, 341, 0)
    end_time = round(time.mktime(t) * 1000)
    return end_time


def get_sorted_pair_csv_over_N(num, start, end):  # receive
    path = Path(Path.home(), "PycharmProjects", Path("Bybit", "Valuta.csv"))
    try:
        if os.path.exists(path):
            os.remove(path)
    except FileNotFoundError:
        print("Path not found")
    for valuta in public_symbols:
        url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol=" \
              f"{valuta}&interval=D&start={start}&end={end}&limit=200"
        payload = {}
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload).json()
        try:
            lst = response['result']['list']
            if len(lst) > 1 or len(lst) == 0:
                print("Больше одного дня ")
                continue
            for i in lst:
                coin_token = valuta.split()
                total = round(float(i[6]))
                if total > num:
                    with open('Valuta.csv', 'a', newline="") as file:
                        writer = csv.writer(file, delimiter=';')
                        writer.writerow(coin_token)
        except KeyError:
            pass


def get_csv_pairs():
    with open('Valuta.csv') as file:
        reader = csv.reader(file)
        for coin in reader:
            coin_str = ''.join(coin)
            lst_coin_over_10kk.append(coin_str)
        return lst_coin_over_10kk


def get_data(coin, time, start, end):
    url = f"https://api.bybit.com/v5/market/kline?category=spot&symbol=" \
          f"{coin}&interval={time}&start={start}&end={end}&limit=200"
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload).json()
    try:
        lst = response['result']['list']
        return lst
    except KeyError:
        print("Надо посмотреть даты! Скорее всего.")


def get_title_for_USDT_csv(filename):
    try:
        with open(path_to_file, 'w', newline="") as file:
            writer = csv.writer(file, delimiter=";")
            writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Total', 'D-Hi', 'D-Low', 'H-L', 'Pairs'])
    except PermissionError:
        print(f"Close a file {filename}")


def write_csv_sorted_data_pairs(data: list, percent: float):
    ind = 0
    for i in data:
        try:
            ind += 1
            if abs(float(i[2]) - float(data[ind][2])) <= (float(i[2]) / 100) * 0.13 and abs(
                    float(i[3]) - float(data[ind][3])) <= (float(i[3]) / 100) * 0.13:
                d_hi, d_low = '+', '+'

            elif abs(float(i[2]) - float(data[ind][2])) <= (float(i[2]) / 100) * 0.13:
                d_hi, d_low = '+', '-'
            elif abs(float(i[3]) - float(data[ind][3])) <= (float(i[3]) / 100) * 0.13:
                d_hi, d_low = '-', '+'
            else:
                d_hi, d_low = '-', '-'
        except IndexError:
            pass
        t = i[0]  # make time
        str_t = float(t[:-3])
        time_server = time.gmtime(str_t)
        time_view = f"{time_server.tm_mday}.{time_server.tm_mon}.{time_server.tm_year}." \
                    f" | {time_server.tm_hour}:{time_server.tm_min}"
        trade_op = i[1]
        trade_open = trade_op.replace('.', ',')
        high = i[2]
        rep_high = high.replace('.', ',')
        low_get = i[3]
        low = low_get.replace('.', ',')
        cls = i[4]
        close = cls.replace('.', ',')
        total = i[6]
        total_trade = total.replace('.', ',')
        high_minus_low = float(i[2]) - float(i[3])
        if ind == 1:
            pair = filename[:-4]
        else:
            pair = ''
        try:
            with open(path_to_file, 'a', newline="", encoding='cp1251') as file:
                writer = csv.writer(file, delimiter=";")
                writer.writerow(
                    (time_view, trade_open, rep_high, low, close, total_trade, d_hi, d_low, high_minus_low, pair))
        except PermissionError:
            pass


def get_title_for_data_csv():
    path = Path(Path.home(), "PycharmProjects", Path("Bybit", "Data.csv"))
    if os.path.exists(path):
        pass
    else:
        with open('Data.csv', 'w', newline="") as file:
            writer = csv.writer(file, delimiter=";")
            writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Total', 'D-Hi', 'D-Low', 'H-L', 'Valuta'])


def get_pair_overall():
    for coin in lst_coin_over_10kk:
        name = coin + '.csv'
        file_name = Path(Path.home(), "PycharmProjects", "Bybit", Path("Data_coins", f"{name}"))
        with open(file_name, newline='') as f:
            reader = csv.reader(f, delimiter=";")
            try:
                row1 = next(reader)  # gets the first line
                row2 = next(reader)  # gets the second line
                t1 = [row2[1]]  #
                t2 = check_to_repeat()
                # print(row2)
                # except StopIteration:
                #     break
                if row2[7] == '+' or row2[6] == '+':
                    for t in t1:
                        for d in t2:
                            if t in d:
                                break
                        else:
                            with open('Data.csv', 'a', newline="") as file:
                                writer = csv.writer(file, delimiter=";")
                                writer.writerow(row2)
            except StopIteration:
                pass


def send_data_to_telegram():
    now = dt.now()
    time_current = f"{now:%d.%m.%Y %H:%M}"
    document = Path(Path.home(), "PycharmProjects", "Bybit", Path("Data.csv"))
    bot = telebot.TeleBot(TELEGRAM_TOKEN)
    bot.send_message(TELEGRAM_CHAT_ID, time_current)
    f = open(document)
    bot.send_document(TELEGRAM_CHAT_ID, f)


if __name__ == '__main__':
    public_symbols = []  # Список для пар USDT
    get_public_symbols()  # Получаем отобранные пары USDT
    start_get_data = time_start(2023, 11, 19)  # С какого дня будет выбраны пары больше 10кк (Разница не более 1 дня)
    end_get_data = time_end(2023, 11, 20)  # По какой день будут выбраны пары больше 10кк
    get_sorted_pair_csv_over_N(1_00_000, start_get_data,
                               end_get_data)  # Принимает даты с разницей в один день и возвращает файл с названием пар с total в день более 10кк
    lst_coin_over_10kk = []
    get_csv_pairs()  # Открывает файл и возвращает список с парами
    for coin in lst_coin_over_10kk:  # Создаем цикл для получения файлов с парами
        filename = coin + '.csv'
        path_to_file = Path(Path.home(), "PycharmProjects", "Bybit", Path("Data_coins", f"{filename}"))
        get_title_for_USDT_csv(filename)  # Создает заголовки для файлов с парами
        start = time_start(2023, 11, 20)  # Year, month, day.
        end = time_end(2023, 11, 22)
        # get_data(i, start, end)
        lst1 = get_data(coin, 15, start, end)  # Получаем список данных с API
        write_csv_sorted_data_pairs(lst1, 0.13)  # Сортируем пары USDT, процент отклонения
    get_title_for_data_csv()  # Создаем заголовки Data.csv
    get_pair_overall()  # Записываем последние пары в файл с +
    send_data_to_telegram()  # Отправляем файл в телеграм бот
