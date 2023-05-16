# -*- coding: utf-8 -*-
import requests

req = requests.get('http://wxmaps.org/pix/temp1.png').content
with open(r'D:\www\A\a\a\img\temp_1.png', 'wb') as f:
    f.write(req)

req = requests.get('http://wxmaps.org/pix/prec1.png').content
with open(r'D:\www\A\a\a\img\prec1_1.png', 'wb') as f:
    f.write(req)

req = requests.get('http://wxmaps.org/pix/soil1.png').content
with open(r'D:\www\A\a\a\img\soil_1.png', 'wb') as f:
    f.write(req)