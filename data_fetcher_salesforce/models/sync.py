import base64
import json
import logging
from typing import Dict, Any, List, Tuple
from odoo.addons.data_fetcher_base.models.odoo_service import OdooService # type: ignore
from odoo.http import UserError # type: ignore
from ..utils.mappers import map_account_to_partner, map_contact_to_partner, map_product_to_odoo, map_lead_to_crm, map_opportunity_to_crm, map_order_to_odoo, map_order_line_to_odoo # type: ignore
from ..utils.helpers import SalesforceHelper # type: ignore
from ..utils.query import fetch_contacts, fetch_employees, fetch_leads, fetch_opportunities, fetch_products, fetch_orders, fetch_order_lines
from odoo import api, fields, models

_logger = logging.getLogger(__name__)
salesforce_helper = SalesforceHelper()

class Sync(models.Model):
    _inherit = 'transfer.log'
    _description = "Salesforce Sync Queue"

    _sf_odoo_id_mapping = {
        'res.partner': {},
        'product.template': {},
        'crm.lead': {},
        'sale.order': {},
        'sale.order.line': {},
    }
    _common_ids = {
        'res.partner': {},
        'res.country': {},
        'res.country.state': {},
        'res.partner.title': {},
        'res.partner.industry': {},
        'utm.source': {},
        'crm.stage': {},
        'product.template': {}
    }

    def sync_all(self, sf_api):
        self.fetch_all_sf_data(sf_api, fetch_contacts(), 'account')
        self.fetch_all_sf_data(sf_api, fetch_employees(), 'contact')
        standard_pricebook_id = self.get_standard_pricebook_id(sf_api)
        self.fetch_all_sf_data(sf_api, fetch_products(standard_pricebook_id), 'product')
        self.fetch_all_sf_data(sf_api, fetch_leads(), 'lead')
        self.fetch_all_sf_data(sf_api, fetch_opportunities(), 'opportunity')
        self.fetch_all_sf_data(sf_api, fetch_orders(), 'order')
        self.fetch_all_sf_data(sf_api, fetch_order_lines(), 'order_line')

    def fetch_all_sf_data(self, sf_api, soql_query: str, sync_type: str, batch_limit: int = 100):
        offset = 0
        total_fetched = 0

        while True:
            records = sf_api.query(soql_query, batch_size=batch_limit, offset=offset)
            if not records:
                break  # No more records

            self.fetch_and_store(records, sync_type)
            total_fetched += len(records)
            offset += batch_limit
            _logger.info(f"Fetched and stored {total_fetched} records for sync type '{sync_type}'")

    def fetch_and_store(self, records: List[Dict[str, Any]], sync_type: str):
        if not records:
            return

        batch_id = f"{sync_type}_batch_{fields.Datetime.now().strftime('%Y%m%d%H%M%S')}"
        sync_record = self.create({
            'name': batch_id,
            'transfer_category': sync_type,
            'sync_status': 'pending',
            'source': 'salesforce',
        })

        # Save entire batch as JSON attachment
        json_str = json.dumps(records)
        encoded = base64.b64encode(json_str.encode('utf-8'))
        self.env['ir.attachment'].create({
            'name': f'batch_{sync_type}_{batch_id}.json',
            'type': 'binary',
            'datas': encoded,
            'res_model': sync_record._name,
            'res_id': sync_record.id,
            'mimetype': 'application/json',
        })
    
    def get_standard_pricebook_id(self, sf_api) -> str:
        """
        Query Salesforce to get the Standard Pricebook2 ID
        """
        soql_query = "SELECT Id FROM Pricebook2 WHERE IsStandard = TRUE"
        result = sf_api.query(
            soql_query, 1, 0
        )
        if not result:
            raise Exception("Standard Pricebook not found in Salesforce.")
        _logger.info(f"Standard Pricebook2 Salesforce ID: {result}")
        return result[0]["Id"]

    @api.model
    def process_sf_queue(self):
        """Cron method to send data"""
        transfers = self.search([('sync_status', 'in', ['pending', 'failed'])])
        _logger.info(f"Pending transfers: {transfers}")

        config = self.env['ir.config_parameter'].sudo()
        url = config.get_param('odoo.url')
        db = config.get_param('odoo.db')
        username = config.get_param('odoo.username')
        password = config.get_param('odoo.password')

        if not all([url, db, username, password]):
            raise UserError("Missing Odoo credentials in system configuration.")
        
        if not transfers:
            _logger.info("No transfers to process.")
            return "No transfers to process."
            
        try:
            odoo_api = OdooService(url, db, username, password)
            odoo_api.connect()
        except Exception as e:
            _logger.error(f"Failed to connect to target Odoo: {e}")
            return "Failed to connect to target Odoo."

        countries = odoo_api.search_read('res.country', [], ['id', 'name'])
        country_map = {country['name'].lower(): country['id'] for country in countries if country.get('name')}

        sync_types = ['account', 'contact', 'product', 'lead', 'opportunity', 'order', 'order_line']
        for sync_type in sync_types:
            type_transfers = transfers.filtered(lambda r: r.transfer_category == sync_type)
            for record in type_transfers:
                try:
                    attachment = self.env['ir.attachment'].search([
                        ('res_model', '=', record._name),
                        ('res_id', '=', record.id),
                        ('name', 'ilike', f'batch_{record.transfer_category}_')
                    ], limit=1)

                    if not attachment:
                        record.write({
                            'sync_status': 'failed',
                            'error_message': 'Batch payload attachment not found'
                        })
                        continue

                    decoded = base64.b64decode(attachment.datas)
                    payload_batch = json.loads(decoded.decode('utf-8'))  # This is now a list
                    processor = getattr(self, f"process_{sync_type}_batch", None)
                    if not processor:
                        _logger.warning(f"No batch processor method defined for sync type '{sync_type}', falling back to individual processing")
                    else:
                        # Use batch processing
                        if sync_type in ['account', 'contact', 'lead']:
                            processor(payload_batch, odoo_api=odoo_api, country_map=country_map)
                        else:
                            processor(payload_batch, odoo_api=odoo_api)
                    record.sync_status = 'completed'

                except Exception as e:
                    record.write({
                        'sync_status': 'failed',
                        'error_message': str(e)
                    })
                    _logger.error(f"Error processing {sync_type} batch: {str(e)}")

    def get_odoo_id(self, model_name, sf_id):
        """Get Odoo ID from Salesforce ID"""
        # Try to get from memory mapping
        if model_name in self._sf_odoo_id_mapping and sf_id in self._sf_odoo_id_mapping[model_name]:
            return self._sf_odoo_id_mapping[model_name][sf_id]
        return None

    def get_industry_id(self, industry_name, odoo_api):
        """Get industry ID locally first, then fallback to RPC call"""
        if not industry_name:
            return None
        
        # Check cached IDs first
        if 'res.partner.industry' in self._common_ids and industry_name in self._common_ids['res.partner.industry']:
            return self._common_ids['res.partner.industry'][industry_name]
        
        # If not in cache, use the original function via RPC
        industry_id = salesforce_helper.get_industry_id(industry_name, odoo_api)
        # Cache the result if found
        if industry_id:
            self._common_ids['res.partner.industry'][industry_name] = industry_id
        return industry_id

    def get_title_id(self, title_name, odoo_api):
        if not title_name:
            return None

        if title_name in self._common_ids['res.partner.title']:
            return self._common_ids['res.partner.title'][title_name]
        
        title_id = salesforce_helper.get_title_id(title_name, odoo_api)
        if title_id:
            self._common_ids['res.partner.title'][title_name] = title_id
        return title_id
    
    def get_source_id(self, source_name, odoo_api):
        if not source_name:
            return None
        
        if source_name in self._common_ids['utm.source']:
            return self._common_ids['utm.source'][source_name]
        
        source_id = odoo_api.get_source_id(source_name)
        if source_id:
            self._common_ids['utm.source'][source_name] = source_id
        return source_id
    
    def get_stage_id(self, stage_name, odoo_api):
        if not stage_name:
            return None
        
        if stage_name in self._common_ids['crm.stage']:
            return self._common_ids['crm.stage'][stage_name]
        
        stage_id = salesforce_helper.get_stage_id(stage_name, odoo_api)
        if stage_id:
            self._common_ids['crm.stage'][stage_name] = stage_id
        return stage_id

    # Batch processing methods
    def process_account_batch(self, data_batch, odoo_api, country_map):
        fields = [
            'name', 'street', 'street2', 'city', 'zip', 'phone', 'email',
            'website', 'company_type', 'is_company', 'ref', "country_id/.id",
            'state_id/.id', 'industry_id/.id', 'comment'
        ]

        rows = []
        sf_ids = []
        for data in data_batch:
            odoo_data = map_account_to_partner(data)
            sf_ids.append(data.get("Id"))

            country = data["BillingCountry"]
            # Get country ID from map
            if country:
                country_id = country_map.get(country) if country else None
                odoo_data["country_id/.id"] = country_id

            state = data["BillingState"]
            # Get state ID from state map
            if country and state:
                odoo_data["state_id/.id"] = odoo_api.get_state_id(country_id, state, state)
      
            industry_name = data["Industry"]
            if industry_name:
                odoo_data["industry_id/.id"] = self.get_industry_id(industry_name, odoo_api)
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)

        if rows:
            result = odoo_api.load_records('res.partner', fields, rows)
            _logger.info(f"Batch import result for accounts: {result}")

            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    self._sf_odoo_id_mapping['res.partner'][sf_id] = odoo_id

    def process_contact_batch(self, data_batch, odoo_api, country_map):
        fields = [
            'name', 'street', 'street2', 'city', 'zip', 'phone', 'email',
            'mobile', 'function', 'title/.id', 'ref', 'parent_id/.id',
            'country_id/.id', 'state_id/.id', 'comment'
        ]
        
        rows = []
        sf_ids = []
        for data in data_batch:
            odoo_data = map_contact_to_partner(data)
            sf_ids.append(data.get("Id"))
            
            country = data["MailingCountry"]
            if country:
                country_id = country_map.get(country) if country else None
                odoo_data["country_id/.id"] = country_id

            state = data["MailingState"]
            if country and state:
                odoo_data["state_id/.id"] = odoo_api.get_state_id(country_id, state, state)

            if data["AccountId"]:
                parent_odoo_id = self.get_odoo_id('res.partner', data["AccountId"])
                if parent_odoo_id:
                    odoo_data['parent_id/.id'] = parent_odoo_id
            
            # Resolve title (Mr., Mrs., etc.)
            salutation = data["Salutation"]
            if salutation:
                title_id = self.get_title_id(salutation, odoo_api)
                odoo_data["title"] = title_id
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        if rows:
            result = odoo_api.load_records('res.partner', fields, rows)
            _logger.info(f"Batch import result for contacts: {result}")

            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    self._sf_odoo_id_mapping['res.partner'][sf_id] = odoo_id

    def process_product_batch(self, data_batch, odoo_api):
        fields = [
            'name', 'description', 'list_price', 'default_code', 'active', 'type'
        ]
        
        rows = []
        sf_ids = []
        product_names = []  # Store product names in same order as sf_ids
        for data in data_batch:
            odoo_data = map_product_to_odoo(data)

            sf_ids.append(data.get("Id"))
            product_names.append(odoo_data.get("name"))  # Store the product name

            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)

        if rows:
            result = odoo_api.load_records('product.template', fields, rows)
            _logger.info(f"Batch import result for products: {result}")
            
            if result and result.get('ids'):
                for sf_id, odoo_id, product_name in zip(sf_ids, result['ids'], product_names):
                    self._sf_odoo_id_mapping['product.template'][sf_id] = odoo_id
                    self._common_ids['product.template'][sf_id] = [odoo_id, product_name]
            
    def process_lead_batch(self, data_batch, odoo_api, country_map):
        fields = [
            'name', 'contact_name', 'function', 'phone', 'mobile', 'email_from',
            'street', 'street2', 'city', 'zip', 'type', 'priority',
            'country_id/.id', 'state_id/.id', 'title/.id', 'source_id/.id',
            'expected_revenue', 'description', 'referred'
        ]

        rows = []
        sf_ids = []
        for data in data_batch:
            odoo_data = map_lead_to_crm(data)
            sf_ids.append(data.get("Id"))

            country = data["Country"]
            if country:
                country_id = country_map.get(country) if country else None
                odoo_data["country_id/.id"] = country_id

            state = data["State"]
            if country and state:
                odoo_data["state_id/.id"] = odoo_api.get_state_id(country_id, state, state)

            salutation = data.get("Salutation")
            if salutation:
                title_id = self.get_title_id(salutation, odoo_api)
                if title_id:
                    odoo_data["title"] = title_id

            source_name = data["LeadSource"]
            if source_name:
                source_id = self.get_source_id(source_name, odoo_api)
                if source_id:
                    odoo_data["source_id"] = source_id
            
            # Calculate expected revenue if AnnualRevenue is available
            annual_revenue = data.get("AnnualRevenue")
            if annual_revenue:
                # Example: 10% of annual revenue as expected_revenue
                odoo_data["expected_revenue"] = float(annual_revenue) * 0.1
            
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        if rows:
            result = odoo_api.load_records('crm.lead', fields, rows)
            _logger.info(f"Batch import result for leads: {result}")

            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    self._sf_odoo_id_mapping['crm.lead'][sf_id] = odoo_id
            
    def process_opportunity_batch(self, data_batch, odoo_api):
        fields = [
            'name', 'partner_id/.id', 'expected_revenue', 'probability',
            'date_deadline', 'type', 'stage_id/.id', 'description', 'referred'
        ]
        
        rows = []
        sf_ids = []
        for data in data_batch:
            odoo_data = map_opportunity_to_crm(data)
            sf_ids.append(data.get("Id"))

            stage_name = data["StageName"]
            if stage_name:
                stage_id = self.get_stage_id(stage_name, odoo_api)
                if stage_id:
                    odoo_data["stage_id/.id"] = stage_id

            partner_id = self.get_odoo_id('res.partner', data.get("AccountId"))
            odoo_data["partner_id/.id"] = partner_id

            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)

        if rows:
            result = odoo_api.load_records('crm.lead', fields, rows)
            _logger.info(f"Batch import result for opportunities: {result}")

            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    self._sf_odoo_id_mapping['crm.lead'][sf_id] = odoo_id
            
    def process_order_batch(self, data_batch, odoo_api):
        fields = [
            'name', 'partner_id/.id', 'date_order', 'validity_date',
            'state'
        ]
        
        rows = []
        sf_ids = []
        for data in data_batch:
            odoo_data = map_order_to_odoo(data)
            sf_ids.append(data.get("Id"))

            partner_id = self.get_odoo_id('res.partner', data.get("AccountId"))
            odoo_data["partner_id/.id"] = partner_id
            
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)

        if rows:
            result = odoo_api.load_records('sale.order', fields, rows)
            _logger.info(f"Batch import result for orders: {result}")

            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    self._sf_odoo_id_mapping['sale.order'][sf_id] = odoo_id
            
    def process_order_line_batch(self, data_batch, odoo_api):
        fields = [
            'virtual_id', 'order_id/.id', 'product_id/.id', 'name',
            'product_uom_qty', 'price_unit', 'discount', 
            'price_subtotal'
        ]
        
        rows = []
        sf_ids = []
        for data in data_batch:
            odoo_data = map_order_line_to_odoo(data)
            sf_ids.append(data.get("Id"))
            
            product_sf_id = data.get("Product2Id")
            if product_sf_id:
                product_id = self.get_odoo_id('product.template', product_sf_id)
                odoo_data["product_id/.id"] = product_id

            order_sf_id = data.get("OrderId")
            if order_sf_id:
                order_id = self.get_odoo_id('sale.order', order_sf_id)
                if order_id:
                    odoo_data["order_id/.id"] = order_id
            
            for key, value in self._common_ids['product.template'].items():
                if isinstance(value, list) and value[0] == product_id:
                    odoo_data["name"] = value[1]

            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        if rows:
            result = odoo_api.load_records('sale.order.line', fields, rows)
            _logger.info(f"Batch import result for order lines: {result}")
            