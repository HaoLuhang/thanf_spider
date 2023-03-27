# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
urls = [
    'https://www.chnmuseum.cn/zp/zpml/csp/yq/index_2.shtml',
    'https://www.chnmuseum.cn/zp/zpml/csp/yq/index_3.shtml',
    'https://www.chnmuseum.cn/zp/zpml/csp/yq/index_4.shtml',
]
headers= {
    'Cookie': '__jsluid_s=6b59c719fda526aa92bf9a3f3a0b2416; _trs_uv=l9p8m8b2_2797_hhfa; Hm_lvt_d8512e191052092ef1dd135588660448=1666764616,1667977776; _trs_ua_s_1=la9awhak_2797_1ike; __jsl_clearance_s=1667981468.001|0|t2CLFw85KNmzBstA4R%2BveDr%2ByPA%3D; Hm_lpvt_d8512e191052092ef1dd135588660448=1667981538',
    'Host': 'www.chnmuseum.cn',
    'Referer': 'https://www.chnmuseum.cn/zp/zpml/csp/yq/index_4.shtml',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
    'sec-ch-ua-mobile': '?0',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36'
}
for url in urls:
    req = requests.get(url,headers=headers)
    print(req)
    soup = BeautifulSoup(req.text, "lxml")
    for img in soup.find('ul',class_='cj_com_zhanchu cj_mb20').find_all('dd'):
        img_url = img.parent.find('dt').find('a').find('img').get('src').replace('..','https://www.chnmuseum.cn/zp/zpml/csp')
        year = img.find('span').get('ndtem').strip().replace('\u3000','')
        if not year:
            year = img.find('span').get('cpsdtem').strip().replace('\u3000','')
        iurl = img.find('a').get('href').replace('..','https://www.chnmuseum.cn/zp/zpml/csp')
        r = requests.get(iurl,headers=headers)
        soup = BeautifulSoup(r.text, "lxml")
        title = soup.find('div', class_='cj_ertitlere').string.strip()
        size = soup.find('div', class_='cj_e_canshu').find('p').string
        print(title,size,year)
        with open(f'D:/project/ParsePDF-master/中国国家博物馆/{year}-{title}-{size}.jpg'.replace('\n',''), 'wb') as f:
            f.write(requests.get(img_url).content)






