import requests
import time
import json
from fake_useragent import UserAgent
import random
from pprint import pprint
import pandas as pd
import os
import math
import pymongo

#请求头
def init_headers():
    headers = {
        'User-Agent':UserAgent().random,
        'Origin-Domain':'www.epubit.com' #爬这个网站必须要这玩意
    }
    return  headers

#从第一页获取总页数
def get_total_page():
    url = 'https://www.epubit.com/pubcloud/content/front/portal/getUbookList?page=1&row=20&=&startPrice=&endPrice=&tagId='
    res = requests.get(url = url, headers = init_headers(), timeout = 10)
    page = res.json()
    page_size = page.get('data').get('size')
    page_total = page.get('data').get('total')
    total_page = math.ceil(page_total / page_size)
    return total_page
#获取当前页面返回json
def get_page(page = 1):
    url = 'https://www.epubit.com/pubcloud/content/front/portal/getUbookList?page={}&row=20&=&startPrice=&endPrice=&tagId='.format(page)
    try:
        res = requests.get(url = url, headers = init_headers(), timeout = 10)
        if res.status_code == 200:
            books = res.json()
            print('已成功请求第{}页'.format(page))
            return books
    except requests.ConnectionError as e:
        print(e)
        print('链接失败')
#用于解析当前页面的json
def parse_books(json_text):
    book_data = []
    if json_text.get('data').get('records'):
        book_records = json_text.get('data').get('records')
        for book in book_records:
            info = {}
            info['书名'] = book.get('name')
            info['作者'] = book.get('authors')
            info['价格'] = book.get('price')
            book_data.append(info)
        return book_data

#存储到csv
def save_to_csv(datas):
    save_file_path = './异步社区书单.csv'
    df = pd.DataFrame(datas)
    df = df.reindex(columns = ['书名','作者','价格'])
    if os.path.exists(save_file_path) and os.path.getsize(save_file_path):
        df.to_csv(save_file_path, mode='a', encoding='utf-8', header=None, index=False)
    else:
        df.to_csv(save_file_path, mode='a', encoding='utf-8', index=False)
        print('已创建' + save_file_path)
#存储到mongodb数据库
def save_to_db(datas):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['epubit']
    collection = db['books']
    collection.insert_many(datas)

if __name__ == '__main__':
    total_page = get_total_page()
    #print(total_page)
    for i in range(1,total_page + 1):
    #for i in range(1,3): #测试
        books = get_page(page=i)
        book_datas = parse_books(books)
        save_to_csv(book_datas)
        save_to_db(book_datas)
        print('共{0}页已存储第{1}页信息'.format(total_page,i))
        time.sleep(random.random()*10) # 防止爬的过快
