import requests
import os
from bs4 import BeautifulSoup 
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime, timedelta
from datetime import date
import time
import re

HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36','accept': '*/*'}

def openDB():
	connection = psycopg2.connect(
							database="Imas_test_task",
							user="zhaslan_Imas",
							password="1010",
							host="127.0.0.1",
							port="5432")
	cursor = connection.cursor()	
	return cursor,connection

def createTables(cursor, connection):
	try:
		cursor.execute(
		'''CREATE TABLE resource  
		(RESOURCE_ID bigserial PRIMARY KEY NOT NULL UNIQUE,
		RESOURCE_NAME CHAR(255) default NULL,
		RESOURCE_URL CHAR(255) NOT NULL UNIQUE,
		top_tag CHAR(255) NOT NULL,
		bottom_tag CHAR(255) NOT NULL,
		title_cut CHAR(255) NOT NULL,
		date_cut CHAR(255) NOT NULL);''')

		cursor.execute(
		'''CREATE TABLE items  
		(id bigserial PRIMARY KEY NOT NULL UNIQUE,
		res_id INT REFERENCES resource (RESOURCE_ID) ,
		link TEXT NOT NULL,
		title TEXT NOT NULL,
		content TEXT NOT NULL,
		nd_date INT NOT NULL,
		s_date INT NOT NULL,
		not_date DATE NOT NULL);''')
		print('таблицы созданы')
	except:
		connection.rollback()
		print('таблицы уже были созданы')

def resourceOut(cursor, connection):
	try:
		cursor.execute(
			"INSERT INTO resource (RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut) VALUES ('NurKZ', 'https://www.nur.kz/latest/', 'article ', 'div formatted-body', 'h1 main-headline', 'time datetime--publication')"
		)
		connection.commit()
		print('Структурa получена')
	except:
		connection.rollback()
		print('Структура уже получена')

	try:
		cursor.execute(
			"INSERT INTO resource (RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut) VALUES ('KazTag', 'https://kaztag.info', 'h2 title', 'div content', 'h1 title', 'div t-info')"
		)
		connection.commit()
		print('Структурa получена')
	except:
		connection.rollback()
		print('Структура уже получена')

def getHtml(url, params=None):
	response = requests.get(url, headers=HEADERS, params=params)
	return response

def getSoup(html):
	soup = BeautifulSoup(html, 'html.parser')
	return soup

def getElements(siteData):
	elements = {'top_tag' : siteData[2].split(),
				'bottom_tag' : siteData[3].split(),
				'title_cut' : siteData[4].split(),
				'date_cut' : siteData[5].split(),}
	for element in elements.values():
		if len(element) == 1:
			element.append(' ')
	return elements

def getUrls(html, elements, soup):
	tagUrl = elements['top_tag']
	if not tagUrl[1]:
		itemsUlrs = soup.find_all(tagUrl[0], class_= tagUrl[1])
	else:
		itemsUlrs = soup.find_all(tagUrl[0])
	urls = []
	for item in itemsUlrs:
		urls.append(item.find_next('a').get('href')),
	return urls

def getSitesData(cursor):
	cursor.execute("SELECT RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut, RESOURCE_ID from resource")
	sitesData = cursor.fetchall()
	return sitesData

def daysCheck(time):
	ymd = time.split(',')
	yesterday = datetime.now() - timedelta(1)
	for i in ymd:
		if i.lower() == 'сегодня':
			return datetime.now().date()
		elif i.lower() == 'вчера':
			return datetime.strftime(yesterday, '%Y-%m-%d')
		elif '-' in i:
			return i.strip().replace('.', '')

def daysCheckUnix():
	pass

def getContent(html, elements, siteData, url, connection, cursor):
	soup = getSoup(html.text)
	titleCut = elements['title_cut']
	bottomTag = elements['bottom_tag']
	dateCut = elements['date_cut']
	title = soup.find(titleCut[0], class_= titleCut[1])
	
	if title == None:
		pass
	else:
		title = title.get_text()
		resId = siteData[6]
		link = url
		title = soup.find(titleCut[0], class_= titleCut[1]).get_text()
		content = soup.find(bottomTag[0], class_= bottomTag[1]).get_text(strip=True)
		ndDate  = 1
		sDate = time.time()
		notDate = daysCheck(soup.find(dateCut[0], class_= dateCut[1]).get_text(strip=True))
		try:
			cursor.execute(
					'''INSERT INTO items (res_id, link, title, content, nd_date, s_date, not_date) VALUES (%s,%s,%s,%s,%s,%s,%s)''',(resId, link, title, content, ndDate, sDate, notDate)
				)
			connection.commit()
		except:
			connection.rollback()
			print('Данные не зафиксированны')

def parse():
	cursor,connection = openDB()
	createTables(cursor, connection)
	resourceOut(cursor, connection)
	sitesData = getSitesData(cursor)
	for siteData in sitesData:
		html = getHtml(siteData[1].strip())
		if html.status_code == 200:
			print('получен ответ от: ', siteData[1].strip())
			elements = getElements(siteData)
			urls = getUrls(html.text, elements, getSoup(html.text))
			for url in urls:
				if 'https://' not in  url:
					url = str(siteData[1].strip() + url)	
					html2 = getHtml(url)
					if html2.status_code == 200:
						getContent(html2, elements, siteData, url, connection, cursor)
					else:
						print('ошибка соединения')
				else:
					html2 = getHtml(url)
					if html2.status_code == 200:
						getContent(html2, elements, siteData, url, connection, cursor)
					else:
						print('ошибка соединения')

		else:
			print('нет ответа от: ', siteData[1].strip())

	cursor.close()
	connection.close()	

parse()
# переписать все по ООП
# написать декораторы для функций
# вытащить структуры для парсинга сайтов в отдельный файл
# разобраться с unix time