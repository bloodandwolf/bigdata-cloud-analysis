#/usr/bin/env python
#coding=utf8
 
import http.client
import hashlib
import urllib
import random
import json
import urllib.parse

def translate(englist_string):
	appKey = '2b9e03711c138b51'
	secretKey = 'qY7DfjdPN8v8VrGsBaeCrNnXxzAQKdXj'

	 
	httpClient = None
	myurl = '/api'
	q = englist_string
	fromLang = 'auto'
	toLang = 'auto'
	salt = random.randint(32768, 65536)

	sign = appKey+q+str(salt)+secretKey
	m1 = hashlib.md5()
	sign=sign.encode('utf-8')
	m1.update(sign)
	sign = m1.hexdigest()
	myurl = myurl+'?appid='+appid+'&q='+urllib.parse.quote(q)+'&from='+fromLang+'&to='+toLang+'&salt='+str(salt)+'&sign='+sign
	 
	try:
	    httpClient = http.client.HTTPConnection('openapi.youdao.com')
	    httpClient.request('GET', myurl)
	 
	    #response是HTTPResponse对象
	    response = httpClient.getresponse()
	    return_string=response.read()
	    return_json=json.loads(return_string)
	    #print(return_json['trans_result'][0]['dst'])
	    return return_json['trans_result'][0]['dst']
	except:
	    return 'null'
	finally:
	    if httpClient:
	        httpClient.close()


if __name__ == '__main__':
	translate('Zimbabwe')
