# -*- coding: utf-8 -*-
import multiprocessing
import requests


def get_img(name):
    f_name = f"分类：{name['classification']}年代：{name['period']}名称：{name['title']}尺寸：{name['dimensions']}"
    print(name)
    with open('D:/project/ParsePDF-master/大都会艺术博物馆/' + f_name.replace('.', '·').replace('/', '~').replace('\r',
                                                                                                                  '').replace(
            '\n', '').replace('?', '') + '.jpg', 'wb') as f:
        f.write(requests.get(name['primaryImage']).content)

if __name__ == "__main__":
    for page in range(5000,12440,40):
        print(page)
        try:
            urls = f'https://www.metmuseum.org/mothra/collectionlisting/search?showOnly=withImage&department=6&geolocation=China&offset={page}&perPage=40'
            req = requests.get(urls)
            pool = multiprocessing.Pool(processes=8)

            for i in (req.json()['results']):
                try:
                    id = i['url'].split('?')[0].split('/')[-1]
                    name = requests.get('https://collectionapi.metmuseum.org/public/collection/v1/objects/'+id).json()
                    if name['primaryImage'] == '':
                        continue
                    print(name)
                    p = pool.apply_async(get_img, (name,))
                except Exception as e:
                    print('error info :',e)
            pool.close()
            pool.join()
        except Exception as e:
            print(e)
            continue

