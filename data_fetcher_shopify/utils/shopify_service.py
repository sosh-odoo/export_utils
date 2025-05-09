# shopify_service.py
import base64
import json
import logging
import requests

_logger = logging.getLogger(__name__)

class ShopifyService:
    """Service class to interact with Shopify API"""
    
    def __init__(self, shopify_url, access_token, env=None):
        """Initialize with Shopify credentials"""
        self.base_url = f"https://{shopify_url}/admin/api/2025-01"
        self.headers = {
            'X-Shopify-Access-Token': access_token,
            'Content-Type': 'application/json'
        }
        self.env = env
    
    def fetch_customers(self, limit=50, page=1):
        """Fetch a page of customers from Shopify"""
        try:
            response = requests.get(
                f"{self.base_url}/customers.json",
                headers=self.headers,
                params={'limit': limit, 'pages': page}
            )
            response.raise_for_status()
            return response.json().get('customers', [])
        except Exception as error:
            _logger.error(f"Error fetching customers from Shopify: {error}")
            raise
    
    def fetch_products(self, limit=50, page=1):
        """Fetch a page of products from Shopify"""
        try:
            response = requests.get(
                f"{self.base_url}/products.json",
                headers=self.headers,
                params={'limit': limit, 'pages': page}
            )
            response.raise_for_status()
            return response.json().get('products', [])
        except Exception as error:
            _logger.error(f"Error fetching products from Shopify: {error}")
            raise
    
    def fetch_orders(self, limit=50, page=1):
        """Fetch a page of orders from Shopify"""
        try:
            response = requests.get(
                f"{self.base_url}/orders.json",
                headers=self.headers,
                params={'status': 'any', 'limit': limit, 'pages': page}
            )
            response.raise_for_status()
            return response.json().get('orders', [])
        except Exception as error:
            _logger.error(f"Error fetching orders from Shopify: {error}")
            raise
    
    def fetch_abandoned_checkouts(self, limit=50, page=1):
        """Fetch a page of abandoned checkouts from Shopify"""
        try:
            response = requests.get(
                f"{self.base_url}/checkouts.json",
                headers=self.headers,
                params={'limit': limit, 'pages': page}
            )
            response.raise_for_status()
            return response.json().get('checkouts', [])
        except Exception as error:
            _logger.error(f"Error fetching abandoned checkouts from Shopify: {error}")
            raise
    
    def fetch_all(self, data_type, chunk_size=250):
        """Generic paginated fetcher for Shopify data."""
        methods = {
            'customers': ('fetch_customers', 'contact'),
            'products': ('fetch_products', 'product'),
            'orders': ('fetch_orders', 'order'),
            'abandoned_checkouts': ('fetch_abandoned_checkouts', 'abandoned_cart'),
        }

        if data_type not in methods:
            raise ValueError(f"Unsupported data type: {data_type}")

        fetch_fn_name, label = methods[data_type]
        fetch_fn = getattr(self, fetch_fn_name)
        
        page = 1
        while True:
            data = fetch_fn(chunk_size, page)
            entries = self._create_transfer_with_attachment(label, page, data)
            print(f"{label.capitalize()} Entries: {entries}")
            if len(data) < chunk_size:
                break
            page += 1

        return f"{label.capitalize()} fetched successfully"

    
    def _create_transfer_with_attachment(self, category, page, data):
        json_str = json.dumps(data)
        encoded = base64.b64encode(json_str.encode('utf-8'))

        log_entry = self.env['transfer.log'].create({
            'name': f'{category.title()} Transfer - Page {page}',
            'sync_status': 'pending',
            'transfer_category': category,
            'source': 'Shopify',
        })

        self.env['ir.attachment'].create({
            'name': f'import_data_{category}_page_{page}.json',
            'type': 'binary',
            'datas': encoded,
            'res_model': log_entry._name,
            'res_id': log_entry.id,
            'mimetype': 'application/json',
        })

        return log_entry
