from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.exceptions import DropItem
from itemloaders.processors import MapCompose
from scrapy.loader import ItemLoader
from scrapy.item import Field
from scrapy.item import Item
import time
import hashlib
import re

# Definición del Item para almacenar la información del producto
class Articulo(Item):
    id = Field()
    titulo = Field()
    precio = Field()
    marca = Field()
    franquicia = Field()
    modelo = Field()
    memoria = Field()
    ram = Field()
    condicion = Field()
    envio = Field()
    fingerprint = Field()
    link = Field()
    ultima_modificacion = Field()

# Definición del Spider para rastrear y extraer datos de Mercadolibre
class MercadolibreCrawl(CrawlSpider):
    name = 'ml_scrapy'

    # Configuración personalizada del spider
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'DOWNLOAD_DELAY': 3, 
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'ROBOTSTXT_OBEY' : False,
        'CONCURRENT_REQUESTS' : 1,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
    }
    items = []
    seen_fingerprints = set()

# Reglas para seguir enlaces y extraer datos
    rules = (
        #Paginas
        Rule(LinkExtractor(
                allow=r'/_Desde_\d+'
            ), follow=True),
        #Productos
        Rule(LinkExtractor(
                allow=r'/MLA'
            ), follow=False, callback='parse'),
    )

# Método para limpiar el campo de condición
    def clean_condition(self, texto):
        nueva_condicion = texto.split('|')[0].strip()
        return nueva_condicion

# Método para procesar la respuesta de la página
    def parse (self, response):
        item = ItemLoader(Articulo(), response)

        item.add_xpath('titulo', '//h1/text()', MapCompose(lambda i: i.replace('\n', ' ').replace('\r', ' ').strip()))
        item.add_xpath('precio', '(//div[@class="ui-pdp-price__second-line"]//span[@class="andes-money-amount__fraction"]/text())[1]')
        item.add_xpath('envio', '(//span[@class="ui-pdp-color--GREEN ui-pdp-family--SEMIBOLD"]/text())[1]')
        item.add_xpath('condicion', '//span[@class="ui-pdp-subtitle"]/text()', MapCompose(self.clean_condition))
        item.add_xpath('memoria', '//div[@class="ui-vpp-highlighted-specs__key-value"]//span[contains(text(), "Memoria interna:")]/following-sibling::span[@class="ui-pdp-color--BLACK ui-pdp-size--XSMALL ui-pdp-family--SEMIBOLD"]/text()')
        item.add_xpath('marca', '//th[div="Marca"]/following-sibling::td/span/text()')
        item.add_xpath('franquicia', '//th[div="Línea"]/following-sibling::td/span/text()')
        item.add_xpath('modelo', '//th[div="Modelo"]/following-sibling::td/span/text()')
        item.add_xpath('ram', '//th[div="Memoria RAM"]/following-sibling::td/span/text()')
        item.add_value('link', response.url)
        item.add_value('ultima_modificacion', int(time.time()))

        regex_pattern = r'(MLA[-]?\d+)|MLA[-]?\d+'
        match = re.search(regex_pattern, response.url)

        if match:
            item.add_value('id', match.group(0)) 
        else:
            raise DropItem("No 'id' field")
        
        if item.get_output_value('precio'):
            # Genera una fingerprint
            combined_str = f"{item.get_output_value('titulo')}|{item.get_output_value('precio')}|{item.get_output_value('id')}"
            hash_object = hashlib.sha256(combined_str.encode())
            fingerprint = hash_object.hexdigest()
            if fingerprint not in self.seen_fingerprints:
                self.seen_fingerprints.add(fingerprint)
                item.add_value('fingerprint', fingerprint)
            else:
                raise DropItem("Same fingerprint already loaded")
        else:
            raise DropItem("Item with missing 'precio' field")

        self.items.append(item.load_item())
    
# Método para obtener la lista de ítems extraídos en formato de diccionario
    def get_items_list(self):
        items_list = []
        for item in self.items:
            item_dict = {
                'condicion': item.get('condicion', [None])[0],
                'envio': item.get('envio', [None])[0],
                'fingerprint': item.get('fingerprint', [None])[0],
                'franquicia': item.get('franquicia', [None])[0],
                'id': item.get('id', [None])[0],
                'link': item.get('link', [None])[0],
                'marca': item.get('marca', [None])[0],
                'memoria': item.get('memoria', [None])[0],
                'modelo': item.get('modelo', [None])[0],
                'precio': item.get('precio', [None])[0],
                'ram': item.get('ram', [None])[0],
                'titulo': item.get('titulo', [None])[0],
                'ultima_modificacion': item.get('ultima_modificacion', [None])[0],
            }
            items_list.append(item_dict) # Añade el ítem convertido en diccionario a la lista
        return items_list
