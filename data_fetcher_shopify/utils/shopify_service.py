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

    def _fetch_data(self, endpoint, key, limit=50, page=1, extra_params=None):
        """Generic helper to fetch data from a Shopify endpoint"""
        params = {'limit': limit, 'pages': page}
        if extra_params:
            params.update(extra_params)

        url = f"{self.base_url}/{endpoint}.json"
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json().get(key, [])
        except Exception as error:
            _logger.error(f"Error fetching {key} from Shopify: {error}")
            raise

    def fetch_customers(self, limit=50, page=1):
        return self._fetch_data('customers', 'customers', limit, page)

    def fetch_products(self, limit=50, page=1):
        return self._fetch_data('products', 'products', limit, page)

    def fetch_orders(self, limit=50, page=1):
        return self._fetch_data('orders', 'orders', limit, page, {'status': 'any'})

    def fetch_abandoned_checkouts(self, limit=50, page=1):
        return self._fetch_data('checkouts', 'checkouts', limit, page)

    def fetch_all(self, log_entry, chunk_size=250):
        """Fetch all relevant Shopify data types and attach to log entry"""
        methods = [
            ('customers', self.fetch_customers, 'customer'),
            ('products', self.fetch_products, 'product'),
            ('orders', self.fetch_orders, 'order'),
            ('abandoned_checkouts', self.fetch_abandoned_checkouts, 'abandoned_cart')
        ]

        for data_type, fetch_fn, label in methods:
            label_title = label.capitalize()
            page = 1
            total_entries = 0

            while True:
                data = fetch_fn(chunk_size, page)
                self._attach_data_to_log(label, page, data, log_entry)
                count = len(data)
                total_entries += count
                _logger.info(f"{label_title} - Page {page} - Entries: {count}")
                if count < chunk_size:
                    break
                page += 1

            _logger.info(f"{label_title} fetched successfully ({total_entries} records)")


    def _attach_data_to_log(self, category, page, data, log_entry):
        """Create an ir.attachment record for the fetched data"""
        encoded = base64.b64encode(json.dumps(data).encode('utf-8'))
        self.env['ir.attachment'].sudo().create({
            'name': f'import_data_{category}_page_{page}.json',
            'type': 'binary',
            'datas': encoded,
            'res_model': log_entry._name,
            'res_id': log_entry.id,
            'description': 'pending',
            'mimetype': 'application/json',
        })
        return log_entry
