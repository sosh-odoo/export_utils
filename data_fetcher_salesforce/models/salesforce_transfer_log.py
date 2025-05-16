import base64
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple
from odoo.addons.data_fetcher_base.models.odoo_service import OdooService # type: ignore
from odoo.http import UserError # type: ignore
from ..utils.mappers import map_account_to_partner, map_contact_to_partner, map_product_to_odoo, map_lead_to_crm, map_opportunity_to_crm, map_order_to_odoo, map_order_line_to_odoo # type: ignore
from ..utils.helpers import SalesforceHelper # type: ignore
from ..utils.query import fetch_contacts, fetch_employees, fetch_leads, fetch_opportunities, fetch_products, fetch_orders, fetch_order_lines
from odoo import api, fields, models

_logger = logging.getLogger(__name__)
salesforce_helper = SalesforceHelper()

class SalesforceTransferLog(models.Model):
    _inherit = 'transfer.log'
    _description = "Salesforce Transfer Log"

    _sf_odoo_id_mapping = {
        'res.partner': {},
        'product.template': {},
        'product.product': {},
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
        'product.template': {},
        'product.product': {}
    }

    def fetch_all(self, sf_api, data):
        fetched_record = self.create({
            'name': self.env.user.name,
            'db_url': data.get('odoo_url'),
            'db_name': data.get('odoo_db'),
            'db_user': data.get('odoo_username'),
            'db_password': data.get('odoo_api_key'),
            'import_status': 'pending',
            'source': 'Salesforce',
        })

        self.fetch_all_sf_data(sf_api, fetch_contacts(), 'account', fetched_record)
        self.fetch_all_sf_data(sf_api, fetch_employees(), 'contact',fetched_record )
        standard_pricebook_id = self.get_standard_pricebook_id(sf_api)
        self.fetch_all_sf_data(sf_api, fetch_products(standard_pricebook_id), 'product', fetched_record)
        self.fetch_all_sf_data(sf_api, fetch_leads(), 'lead', fetched_record)
        self.fetch_all_sf_data(sf_api, fetch_opportunities(), 'opportunity', fetched_record)
        self.fetch_all_sf_data(sf_api, fetch_orders(), 'order', fetched_record)
        self.fetch_all_sf_data(sf_api, fetch_order_lines(), 'orderline', fetched_record)

    def fetch_all_sf_data(self, sf_api, soql_query: str, transfer_category: str, fetched_record, batch_limit: int = 200):
        offset = 0
        total_fetched = 0

        while True:
            records = sf_api.query(soql_query, batch_size=batch_limit, offset=offset)
            if not records:
                break  # No more records

            self.fetch_and_store(records, transfer_category, fetched_record)
            total_fetched += len(records)
            offset += batch_limit
            _logger.info(f"Fetched and stored {total_fetched} records for transfer category '{transfer_category}'")

    def fetch_and_store(self, records: List[Dict[str, Any]], transfer_category: str, fetched_record):
        if not records:
            return

        batch_time = f"{fields.Datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Save entire batch as JSON attachment
        json_str = json.dumps(records)
        encoded = base64.b64encode(json_str.encode('utf-8'))
        self.env['ir.attachment'].sudo().create({
            'name': f'batch_{transfer_category}_{fetched_record.db_user}_{batch_time}.json',
            'type': 'binary',
            'datas': encoded,
            'res_model': fetched_record._name,
            'res_id': fetched_record.id,
            'mimetype': 'application/json',
            'description': 'pending'
        })
    
    def get_standard_pricebook_id(self, sf_api) -> str:
        """
        Query Salesforce to get the Standard Pricebook2 ID
        """
        result = sf_api.query(
            "SELECT Id FROM Pricebook2 WHERE IsStandard = TRUE", 1, 0
        )
        if not result:
            raise Exception("Standard Pricebook not found in Salesforce.")
        _logger.info(f"Standard Pricebook2 Salesforce ID: {result}")
        return result[0]["Id"]

    @api.model
    def process_sf_queue(self):
        """Cron method to send Salesforce data"""
        transfers = self.search([
            ('import_status', '=', 'pending'),
            ('source', '=', 'Salesforce')
        ])
        _logger.info(f"Found {len(transfers)} pending Salesforce transfers.")

        if not transfers:
            _logger.info("No transfers to process.")
            return "No transfers to process."

        for transfer in transfers:
            transfer.import_status = 'in_progress'
            failed_categories = set()
            transfer_categories = ['account', 'contact', 'product', 'lead', 'opportunity', 'order', 'orderline']

            try:
                # Extract and validate credentials
                url = transfer.db_url
                db = transfer.db_name
                username = transfer.db_user
                password = transfer.db_password

                if not all([url, db, username, password]):
                    transfer.import_status = 'failed'
                    transfer.error_message = "Missing Odoo credentials."
                    _logger.error(f"[{transfer.name}] Transfer failed: Missing credentials.")
                    continue

                # Connect to Odoo
                odoo_api = OdooService(url, db, username, password)
                odoo_api.connect()

                for transfer_category in transfer_categories:
                    attachments = self.env['ir.attachment'].search([
                        ('res_model', '=', transfer._name),
                        ('res_id', '=', transfer.id),
                        ('name', 'ilike', f'batch_{transfer_category}_{username}'),
                        ('description', '=', 'pending'),
                    ])

                    if not attachments:
                        _logger.warning(f"[{transfer.name}] No attachments for transfer category '{transfer_category}'. Skipping.")
                        continue

                    category_failed = False

                    for attachment in attachments:
                        try:
                            decoded = base64.b64decode(attachment.datas)
                            payload_batch = json.loads(decoded.decode('utf-8'))
                        except Exception as decode_err:
                            _logger.error(f"[{transfer.name}] Failed to decode {attachment.name}: {decode_err}", exc_info=True)
                            attachment.write({'description': 'failed'})
                            category_failed = True
                            continue

                        try:
                            processor = getattr(self, f"process_{transfer_category}_batch", None)
                            if not processor:
                                _logger.warning(f"[{transfer.name}] No processor found for '{transfer_category}'. Skipping.")
                                category_failed = True
                                attachment.write({'description': 'failed'})
                                continue

                            # Batch processing call
                            result = processor(payload_batch, odoo_api=odoo_api)

                            messages = result.get('messages', [])
                            if result.get('ids') and not messages:
                                attachment.write({'description': 'completed'})
                                _logger.info(f"[{transfer.name}] Attachment {attachment.name} processed successfully.")
                                attachment.unlink()
                            else:
                                message_text = "\n".join(
                                        [m if isinstance(m, str) else json.dumps(m) for m in messages]
                                ) if messages else "No result IDs returned." 
                                _logger.warning(f"[{transfer.name}] Incomplete result for {transfer_category}: {message_text}")
                                attachment.write({'description': 'failed'})
                                category_failed = True

                        except Exception as import_err:
                            _logger.error(f"[{transfer.name}] Error importing {transfer_category}: {import_err}", exc_info=True)
                            attachment.write({'description': 'failed'})
                            category_failed = True
                            continue

                    if category_failed:
                        failed_categories.add(transfer_category)

                # Finalize status
                if len(failed_categories) == len(transfer_categories):
                    transfer.import_status = 'failed'
                    transfer.error_message = f"All Transfer Categories failed: {', '.join(failed_categories)}"
                    _logger.error(f"[{transfer.name}] Complete failure.")
                elif failed_categories:
                    transfer.import_status = 'completed'
                    transfer.error_message = f"Partial failure in: {', '.join(failed_categories)}"
                    _logger.warning(f"[{transfer.name}] Partial success.")
                else:
                    transfer.import_status = 'completed'
                    transfer.error_message = False
                    transfer.import_date = datetime.now()
                    _logger.info(f"[{transfer.name}] Transfer completed successfully.")
                transfer.db_url = False
                transfer.db_password = False
            except Exception as e:
                transfer.import_status = 'failed'
                transfer.error_message = str(e)
                _logger.error(f"[{transfer.name}] Transfer failed: {e}", exc_info=True)

            finally:
                self.env['ir.attachment'].create({
                    'name': f"salesforce_odoo_mapping_{transfer.name}_{transfer.db_user}.json",
                    'type': 'binary',
                    'res_model': transfer._name,
                    'res_id': transfer.id,
                    'datas': base64.b64encode(json.dumps(self._sf_odoo_id_mapping, indent=2).encode('utf-8')),
                    'mimetype': 'application/json',
                    'description': 'Mapping of Salesforce to Odoo IDs',
                })
                # Initialize/Reset class-level mappings at the start of processing THIS transfer.
                for key in self._sf_odoo_id_mapping:
                    self._sf_odoo_id_mapping[key].clear()
                for key in self._common_ids:
                    self._common_ids[key].clear()

    def get_odoo_id(self, model_name, sf_id):
        """Get Odoo ID from Salesforce ID"""
        # Try to get from memory mapping
        if model_name in self._sf_odoo_id_mapping and sf_id in self._sf_odoo_id_mapping[model_name]:
            return self._sf_odoo_id_mapping[model_name][sf_id]
        return None

    def get_id(self, model_name, name_value, odoo_api, id=None):
        if not name_value:
            return None

        if name_value in self._common_ids[model_name]:
            return self._common_ids[model_name][name_value]

        model_to_key_map = {
            'res.partner.title': 'title',
            'utm.source': 'source',
            'crm.stage': 'stage',
            'res.country': 'country',
            'res.country.state' : 'state',
            'res.partner.industry': 'industry',
        }
        key_part = model_to_key_map.get(model_name)
        method_to_call_name = f"get_{key_part}_id"
        record_id = None

        try:
            fetcher_method = getattr(salesforce_helper, method_to_call_name)
            if key_part == 'state':
                record_id = fetcher_method(name_value, odoo_api, id)
            else:
                record_id = fetcher_method(name_value, odoo_api)

        except Exception as e:
            print(f"Error calling '{method_to_call_name}' for '{name_value}' in model '{model_name}': {e}")

        if record_id:
            self._common_ids[model_name][name_value] = record_id
        return record_id

    # Batch processing methods
    def process_account_batch(self, data_batch, odoo_api):
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
                country_id = self.get_id('res.country', country, odoo_api)
                odoo_data["country_id/.id"] = country_id

            state = data["BillingState"]
            # Get state ID from state map
            if country and state:
                odoo_data["state_id/.id"] = self.get_id('res.country.state', state, odoo_api, country_id)
      
            industry_name = data["Industry"]
            if industry_name:
                odoo_data["industry_id/.id"] = self.get_id('res.partner.industry', industry_name, odoo_api)

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
        return result

    def process_contact_batch(self, data_batch, odoo_api):
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
                country_id = self.get_id('res.country', country, odoo_api)
                odoo_data["country_id/.id"] = country_id

            state = data["MailingState"]
            if country and state:
                odoo_data["state_id/.id"] = self.get_id('res.country.state', state, odoo_api, country_id)

            if data["AccountId"]:
                odoo_data['parent_id/.id'] = self.get_odoo_id('res.partner', data["AccountId"])
            
            # Resolve title (Mr., Mrs., etc.)
            salutation = data["Salutation"]
            if salutation:
                odoo_data["title"] = self.get_id('res.partner.title', salutation, odoo_api)

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
        return result
    
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

                template_ids = result['ids']
                template_id_mapping = {}  # Map template ID to SF ID for quick lookup
                
                for sf_id, odoo_id, product_name in zip(sf_ids, template_ids, product_names):
                    self._sf_odoo_id_mapping['product.template'][sf_id] = odoo_id
                    template_id_mapping[odoo_id] = sf_id
                    self._common_ids['product.template'][sf_id] = [odoo_id, product_name]

                # Batch fetch all the product variants for these template_ids
                try:
                    domain = [('product_tmpl_id', 'in', template_ids)]
                    fields_to_fetch = ['id', 'product_tmpl_id']
                    product_variants = odoo_api.search_read('product.product', domain, fields_to_fetch)

                    for variant in product_variants:
                        tmpl_id = variant['product_tmpl_id']

                        if isinstance(tmpl_id, (list, tuple)):
                            name = tmpl_id[1]
                            tmpl_id = tmpl_id[0]  # Get the actual ID (first element) 

                        sf_id = template_id_mapping[tmpl_id]
                        self._sf_odoo_id_mapping['product.product'][sf_id] = variant['id']
                        self._common_ids['product.product'][sf_id] = [variant['id'], name]

                except Exception as error:
                    _logger.error(f"Error fetching product variants in batch: {str(error)}")

        return result
        
    def process_lead_batch(self, data_batch, odoo_api):
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
                country_id = self.get_id('res.country', country, odoo_api)
                odoo_data["country_id/.id"] = country_id

            state = data["State"]
            if country and state:
                odoo_data["state_id/.id"] = self.get_id('res.country.state', state, odoo_api, country_id)

            salutation = data.get("Salutation")
            if salutation:
                odoo_data["title"] = self.get_id('res.partner.title', salutation, odoo_api)
                
            source_name = data["LeadSource"]
            if source_name:
                odoo_data["source_id/.id"] = self.get_id('utm.source', source_name, odoo_api)
            
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
        return result
        
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
                odoo_data["stage_id/.id"] = self.get_id('crm.stage', stage_name, odoo_api)

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
        return result
        
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
        return result
        
    def process_orderline_batch(self, data_batch, odoo_api):
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
                product_id = self.get_odoo_id('product.product', product_sf_id)
                odoo_data["product_id/.id"] = product_id

            order_sf_id = data.get("OrderId")
            if order_sf_id:
                order_id = self.get_odoo_id('sale.order', order_sf_id)
                if order_id:
                    odoo_data["order_id/.id"] = order_id
            
            for key, value in self._common_ids['product.product'].items():
                if isinstance(value, list) and value[0] == product_id:
                    odoo_data["name"] = value[1]

            row = []
            for field in fields:
                row.append(odoo_data.get(field, ''))
            rows.append(row)
        
        if rows:
            result = odoo_api.load_records('sale.order.line', fields, rows)
            _logger.info(f"Batch import result for order lines: {result}")

            if result and result.get('ids'):
                for sf_id, odoo_id in zip(sf_ids, result['ids']):
                    self._sf_odoo_id_mapping['sale.order.line'][sf_id] = odoo_id
        return result
    