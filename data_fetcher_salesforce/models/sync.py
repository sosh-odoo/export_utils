import base64
import json
import logging
from typing import Dict, Any, List, Tuple
from odoo.addons.data_fetcher_base.models.odoo_service import OdooService # type: ignore
from ..utils.mappers import map_account_to_partner, map_contact_to_partner, map_product_to_odoo, map_lead_to_crm, map_opportunity_to_crm, map_order_to_odoo, map_order_line_to_odoo, map_sf_invoice_to_odoo, map_invoice_line_to_odoo # type: ignore
from ..utils.helpers import SalesforceHelper # type: ignore
from ..utils.query import Query
from odoo import api, fields, models

logger = logging.getLogger(__name__)
salesforce_helper = SalesforceHelper()

class Sync(models.Model):
    _inherit = 'transfer.log'
    _description = "Salesforce Sync Queue"

    _sf_odoo_id_mapping = {}

    _common_ids = {
        'res.partner': {},
        'res.country': {},
        'res.country.state': {},
        'res.partner.title': {},
        'utm.source': {},
        'crm.stage': {},
        'product.template': {}
    }

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

            logger.info(f"Fetched and stored {total_fetched} records for sync type '{sync_type}'")

    def sync_all(self, sf_api):
        self.fetch_all_sf_data(sf_api, Query.fetch_contacts(), 'account')
        self.fetch_all_sf_data(sf_api, Query.fetch_employees(), 'contact')
        self.fetch_all_sf_data(sf_api, Query.fetch_products(), 'product')
        self.fetch_all_sf_data(sf_api, Query.fetch_leads(), 'lead')
        self.fetch_all_sf_data(sf_api, Query.fetch_opportunities(), 'opportunity')
        self.fetch_all_sf_data(sf_api, Query.fetch_orders(), 'order')
        self.fetch_all_sf_data(sf_api, Query.fetch_order_lines(), 'order_line')
        # self.fetch_all_sf_data(sf_api, Query.fetch_invoices(), 'invoice')
        # self.fetch_all_sf_data(sf_api, Query.fetch_invoice_lines(), 'invoice_line')
        

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
    
    @api.model
    def process_sf_queue(self):
        config = self.env['ir.config_parameter'].sudo()
        url = config.get_param('odoo.url')
        db = config.get_param('odoo.db')
        username = config.get_param('odoo.username')
        password = config.get_param('odoo.password')
        odoo_api = OdooService(url, db, username, password)
        odoo_api.connect()

        sync_types = ['account', 'contact', 'product', 'lead', 'opportunity', 'order', 'order_line']
        for sync_type in sync_types:
            batch_records = self.search([
                ('transfer_category', '=', sync_type),
                '|',
                ('sync_status', '=', 'pending'),
                ('sync_status', '=', 'failed')
            ])

            for record in batch_records:
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
                        logger.warning(f"No batch processor method defined for sync type '{sync_type}', falling back to individual processing")
                    else:
                        # Use batch processing
                        result = processor(payload_batch, odoo_api=odoo_api)
                        print(result)
                    record.sync_status = 'completed'

                except Exception as e:
                    record.write({
                        'sync_status': 'failed',
                        'error_message': str(e)
                    })
                    logger.error(f"Error processing {sync_type} batch: {str(e)}")
    
    def _store_id_mapping(self, model_name, sf_ids, odoo_ids):
        """Store ID mappings in a database table for persistence across sessions"""
        # Check if we have a dedicated mapping model, if not, create mappings in memory only
        if hasattr(self.env, 'sf_odoo_mapping'):
            mapping_model = self.env['sf.odoo.mapping']
            for sf_id, odoo_id in zip(sf_ids, odoo_ids):
                if sf_id and odoo_id:
                    # Check if mapping already exists
                    existing = mapping_model.search([
                        ('salesforce_id', '=', sf_id),
                        ('model', '=', model_name)
                    ], limit=1)
                    
                    if existing:
                        existing.write({'odoo_id': odoo_id})
                    else:
                        mapping_model.create({
                            'salesforce_id': sf_id,
                            'odoo_id': odoo_id,
                            'model': model_name
                        })

    def get_odoo_id(self, model_name, sf_id):
        """Get Odoo ID from Salesforce ID"""
        # Try to get from memory mapping first
        if model_name in self._sf_odoo_id_mapping and sf_id in self._sf_odoo_id_mapping[model_name]:
            return self._sf_odoo_id_mapping[model_name][sf_id]
        
        # If not in memory, try to get from database if we have a mapping model
        if hasattr(self.env, 'sf_odoo_mapping'):
            mapping = self.env['sf.odoo.mapping'].search([
                ('salesforce_id', '=', sf_id),
                ('model', '=', model_name)
            ], limit=1)
            
            if mapping:
                # Update in-memory mapping for faster lookup next time
                if model_name not in self._sf_odoo_id_mapping:
                    self._sf_odoo_id_mapping[model_name] = {}
                self._sf_odoo_id_mapping[model_name][sf_id] = mapping.odoo_id
                return mapping.odoo_id

        return None

    def get_title_id(self, title_name, odoo_api):
        """Get title ID locally first, then fallback to RPC call"""
        if not title_name:
            return None
        
        # Check cached IDs first
        if title_name in self._common_ids['res.partner.title']:
            return self._common_ids['res.partner.title'][title_name]
        
        # If not in cache, use the original function via RPC
        title_id = odoo_api.get_title_id(title_name)
        
        # Cache the result if found
        if title_id:
            self._common_ids['res.partner.title'][title_name] = title_id
        
        return title_id
    
    def get_source_id(self, source_name, odoo_api):
        """Get source ID locally first, then fallback to RPC call"""
        if not source_name:
            return None
        
        # Check cached IDs first
        if source_name in self._common_ids['utm.source']:
            return self._common_ids['utm.source'][source_name]
        
        # If not in cache, use the original function via RPC
        source_id = odoo_api.get_source_id(source_name)
        
        # Cache the result if found
        if source_id:
            self._common_ids['utm.source'][source_name] = source_id
        
        return source_id
    
    def get_stage_id(self, stage_name, odoo_api):
        """Get stage ID locally first, then fallback to RPC call"""
        if not stage_name:
            return None
        
        # Check cached IDs first
        if stage_name in self._common_ids['crm.stage']:
            return self._common_ids['crm.stage'][stage_name]
        
        # If not in cache, use the helper function
        stage_id = salesforce_helper.get_stage_id(odoo_api, stage_name)
        
        # Cache the result if found
        if stage_id:
            self._common_ids['crm.stage'][stage_name] = stage_id
        
        return stage_id

    # Batch processing methods
    def process_account_batch(self, data_batch, odoo_api):
        """Process a batch of Salesforce accounts using load method"""
        if not data_batch:
            return
            
        # Define fields for account import
        fields = [
            'name', 'street', 'street2', 'city', 'zip', 'phone', 'email',
            'website', 'company_type', 'is_company', 'ref', "country_id/.id",
            'state_id/.id', 'industry_id/.id', 'comment'
        ]
        
        rows = []
        sf_ids = []
        for data in data_batch:
            odoo_data = map_account_to_partner(data)
            # Store the SF ID for later mapping
            sf_ids.append(data.get("Id"))
            # Prepare row for import
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        # Import data in a batch
        if rows:
            result = odoo_api.load('res.partner', fields, rows)
            logger.info(f"Batch import result for accounts: {result}")

            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    if sf_id and odoo_id:
                        if 'res.partner' not in self._sf_odoo_id_mapping:
                            self._sf_odoo_id_mapping['res.partner'] = {}
                        self._sf_odoo_id_mapping['res.partner'][sf_id] = odoo_id
                
                # Optionally, also store the mapping in a more persistent way
                self._store_id_mapping('res.partner', sf_ids, result['ids'])

    def process_contact_batch(self, data_batch, odoo_api):
        """Process a batch of Salesforce contacts using load method"""
        if not data_batch:
            return
            
        # Define fields for contact import
        fields = [
            'name', 'street', 'street2', 'city', 'zip', 'phone', 'email',
            'mobile', 'function', 'title/.id', 'ref', 'parent_id/.id',
            'country_id/.id', 'state_id/.id', 'comment'
        ]
        
        rows = []
        sf_ids = []
        
        for data in data_batch:
            odoo_data = map_contact_to_partner(data)
            
            # If there's a parent company (AccountId), look up its Odoo ID
            if odoo_data['parent_id/.id']:
                parent_odoo_id = self.get_odoo_id('res.partner', odoo_data['parent_id/.id'])
                if parent_odoo_id:
                    odoo_data['parent_id/.id'] = parent_odoo_id
            sf_ids.append(data.get("Id"))
            
            # Resolve title (Mr., Mrs., etc.)
            salutation = data["Salutation"]
            if salutation:
                title_id = self.get_title_id(salutation, odoo_api)
                if title_id:
                    odoo_data["title"] = title_id

            # Prepare row for import
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        # Import data in a batch
        if rows:
            result = odoo_api.load('res.partner', fields, rows)
            logger.info(f"Batch import result for contacts: {result}")
            
            # Update the mapping dictionary with the new IDs
            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    if sf_id and odoo_id:
                        if 'res.partner' not in self._sf_odoo_id_mapping:
                            self._sf_odoo_id_mapping['res.partner'] = {}
                        self._sf_odoo_id_mapping['res.partner'][sf_id] = odoo_id
                
                # Optionally, also store the mapping in a more persistent way
                self._store_id_mapping('res.partner', sf_ids, result['ids'])
                
            return result

    def process_product_batch(self, data_batch, odoo_api):
        """Process a batch of Salesforce products using load method"""
        if not data_batch:
            return
            
        # Define fields for product import
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

            # Prepare row for import
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        # Import data in a batch
        if rows:
            result = odoo_api.load('product.template', fields, rows)
            logger.info(f"Batch import result for products: {result}")
            
            # Update the mapping dictionary with the new IDs
            if result and result.get('ids'):
                for sf_id, odoo_id, product_name in zip(sf_ids, result['ids'], product_names):
                    if 'product.template' not in self._sf_odoo_id_mapping:
                        self._sf_odoo_id_mapping['product.template'] = {} 
                    self._sf_odoo_id_mapping['product.template'][sf_id] = odoo_id
                    self._common_ids['product.template'][sf_id] = [odoo_id, product_name]

                # Optionally, also store the mapping in a more persistent way
                self._store_id_mapping('product.template', sf_ids, result['ids'])
            
    def process_lead_batch(self, data_batch, odoo_api):
        """Process a batch of Salesforce leads using load method"""
        if not data_batch:
            return
            
        # Define fields for lead import
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
            # Handle salutation/title
            salutation = data.get("Salutation")
            if salutation:
                title_id = self.get_title_id(salutation, odoo_api)
                if title_id:
                    odoo_data["title"] = title_id
            
            # Resolve lead source if present
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
            
            # Prepare row for import
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        # Import data in a batch
        if rows:
            result = odoo_api.load('crm.lead', fields, rows)
            logger.info(f"Batch import result for leads: {result}")
            
            # Update the mapping dictionary with the new IDs
            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    if sf_id and odoo_id:
                        if 'crm.lead' not in self._sf_odoo_id_mapping:
                            self._sf_odoo_id_mapping['crm.lead'] = {}
                        self._sf_odoo_id_mapping['crm.lead'][sf_id] = odoo_id
            
            return result
            
    def process_opportunity_batch(self, data_batch, odoo_api):
        """Process a batch of Salesforce opportunities using load method"""
        if not data_batch:
            return
            
        # Define fields for opportunity import
        fields = [
            'name', 'partner_id/.id', 'expected_revenue', 'probability',
            'date_deadline', 'type', 'stage_id/.id', 'description', 'referred'
        ]
        
        rows = []
        sf_ids = []
        for data in data_batch:
            odoo_data = map_opportunity_to_crm(data)
            sf_ids.append(data.get("Id"))

            # Handle opportunity stage
            stage_name = data["StageName"]
            if stage_name:
                stage_id = self.get_stage_id(odoo_api, stage_name)
                if stage_id:
                    odoo_data["stage_id/.id"] = stage_id

            partner_id = self.get_odoo_id('res.partner', data.get("AccountId"))
            odoo_data["partner_id/.id"] = partner_id
            
            # Prepare row for import
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        # Import data in a batch
        if rows:
            result = odoo_api.load('crm.lead', fields, rows)
            logger.info(f"Batch import result for opportunities: {result}")
            
            # Update the mapping dictionary with the new IDs
            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    if sf_id and odoo_id:
                        if 'crm.lead' not in self._sf_odoo_id_mapping:
                            self._sf_odoo_id_mapping['crm.lead'] = {}
                        self._sf_odoo_id_mapping['crm.lead'][sf_id] = odoo_id
            
            return result
            
    def process_order_batch(self, data_batch, odoo_api):
        """Process a batch of Salesforce orders using load method"""
        if not data_batch:
            return
            
        # Define fields for order import
        fields = [
            'name', 'partner_id/.id', 'date_order', 'validity_date',
            'state'
        ]
        
        rows = []
        sf_ids = []

        for data in data_batch:
            odoo_data = map_order_to_odoo(data)
            sf_ids.append(data.get("Id"))

            # Handle partner reference
            partner_id = self.get_odoo_id('res.partner', data.get("AccountId"))
            odoo_data["partner_id/.id"] = partner_id
            
            # Prepare row for import
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        # Import data in a batch
        if rows:
            result = odoo_api.load('sale.order', fields, rows)
            logger.info(f"Batch import result for orders: {result}")

            # Update the mapping dictionary with the new IDs
            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    if sf_id and odoo_id:
                        self._sf_odoo_id_mapping['sale.order'][sf_id] = odoo_id

                # Optionally, also store the mapping in a more persistent way
                self._store_id_mapping('sale.order', sf_ids, result['ids'])

            return result
            
    def process_order_line_batch(self, data_batch, odoo_api):
        """Process a batch of Salesforce order lines using load method"""
        if not data_batch:
            return
            
        # Define fields for order line import
        fields = [
            'virtual_id', 'order_id/.id', 'product_id/.id', 'name',
            'product_uom_qty', 'price_unit', 'tax_id/id', 'discount', 
            'price_subtotal'
        ]
        
        rows = []
        sf_ids = []
        for data in data_batch:
            odoo_data = map_order_line_to_odoo(data)
            sf_ids.append(data.get("Id"))
            
            product_sf_id = data.get("Product2Id")
            if product_sf_id:
                # Check if we have this product in our mapping
                product_id = self.get_odoo_id('product.template', product_sf_id)
                odoo_data["product_id/.id"] = product_id

            # Handle order connection
            order_sf_id = data.get("OrderId")
            if order_sf_id:
                # Check if we have this order in our mapping
                order_id = self.get_odoo_id('sale.order', order_sf_id)
                if order_id:
                    odoo_data["order_id/.id"] = order_id
            
            for key, value in self._common_ids['product.template'].items():
                if isinstance(value, list) and value[0] == product_id:
                    odoo_data["name"] = value[1]

            # Prepare row for import
            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        # Import data in a batch
        if rows:
            result = odoo_api.load('sale.order.line', fields, rows)
            logger.info(f"Batch import result for order lines: {result}")
            return result
            