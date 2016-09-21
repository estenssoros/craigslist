from browser import start_browser
from bs4 import BeautifulSoup



if __name__ == '__main__':
    url = "http://www.zillow.com/homes/for_rent/house,mobile_type/1-_baths/0-569616_price/0-2000_mp/1_pets/39.799172,-104.841328,39.647072,-105.061055_rect/11_zm/"
    br = start_browser(url)
    html = br.response().read()
    # soup = BeautifulSoup(html,'html.parser')
    data = html.split('data:{zpid:[')[1].split(']')[0]
    ids = data.split(',')
    for id in ids:
        print id
