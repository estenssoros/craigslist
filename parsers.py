import re
from dateutil import parser
from bs4 import BeautifulSoup

def parse_info(info):
    posted, updated = info
    posted = parser.parse(posted.split('ed:')[1]).date()
    if updated:
        updated = parser.parse(updated.split('ed:')[1]).date()
    return posted, updated

def parse_attrs(attrs):
    bed = None
    bath = None
    sqft = None
    available = None
    laundry = None
    apartment = 'No'
    attrs = [a.lower() for a in attrs]
    for a in attrs:
        if '/' in a and 'br' in a:
            bed = re.findall('([0-9a-z]+)br', a)[0]
            bath = re.findall('([0-9.a-z]+)ba', a)[0]
        elif 'ft2' in a:
            sqft = int(a.split('ft2')[0])
        elif 'available' in a:
            available = a
        elif 'laundry' in a:
            laundry = a
        elif 'apartment' in a:
            apartment = 'Yes'
    return bed, bath, sqft, available, laundry, apartment

def parse_html(html):
    with open('test.html','w') as f:
        f.write(html)
    lat = None
    lon = None
    price = None
    housing = None
    info = None
    posted = None
    updated = None
    img = None

    soup = BeautifulSoup(html, 'html.parser')

    maps = soup.findAll('div', {'id': 'map'})
    if len(maps) == 1:
        lat = maps[0].attrs['data-latitude']
        lon = maps[0].attrs['data-longitude']
    elif len(maps) > 1:
        raise ValueError('more than one mapbox?')
    price = soup.find('span', {'class': 'price'}).text

    info = soup.findAll('p', {'class': 'postinginfo reveal'})
    for i in info:
        if 'posted' in i.text.lower():
            posted = i.text
        elif 'updated' in i.text.lower():
            updated = i.text

    attrs_group = []
    attrs = soup.findAll('p', {'class': 'attrgroup'})
    for a in attrs:
        spans = a.findAll('span')
        for s in spans:
            attrs_group.append(s.text)
    image = soup.find('img')['src']
    dic = {'coord': (lat, lon), 'price': (price,), 'info': (posted, updated), 'attributes': tuple(attrs_group),'image':image}
    return dic

if __name__ == '__main__':
    pass
