# -*- coding: utf-8 -*-
import xmlrpc.client
import logging
import ssl
from typing import Dict, Any, List, Optional, Tuple
import os
import json

_logger = logging.getLogger(__name__)

class OdooService:
    _state_map = None
    _state_map_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state_map.json')
    
    def __init__(self, url, db, username, api_key):
        """Initialize connection parameters"""
        self.url = url
        self.db = db
        self.username = username
        self.api_key = api_key
        self.uid = None

    def connect(self):
        """Connect to Odoo"""
        try:
            # Create XML-RPC connection for authentication
            common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common', context=ssl._create_unverified_context())
            
            # Authenticate to get UID
            self.uid = common.authenticate(self.db, self.username, self.api_key, {})
            if not self.uid:
                _logger.error(f'Failed to authenticate to Odoo at {self.url}')
                return False
                
            # Create XML-RPC connection for models
            self.models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object', context=ssl._create_unverified_context(), allow_none=True)
            
            _logger.info(f'Connected to Odoo at {self.url}')
            return True
        except Exception as error:
            _logger.error(f'Error connecting to Odoo: {str(error)}')
            raise error

    def search_read(self, model, domain, fields, limit=None, offset=0):
        """Search and read records"""
        try:
            result = self.models.execute_kw(
                self.db, self.uid, self.api_key,
                model, 'search_read',
                [domain, fields]
            )
            return result
        except Exception as error:
            _logger.error(f'Error searching {model} in Odoo: {str(error)}')
            raise error

    def create_record(self, model, data):
        """Create a record"""
        try:
            record_id = self.models.execute_kw(
                self.db, self.uid, self.api_key,
                model, 'create',
                [data]
            )
            return record_id
        except Exception as error:
            _logger.error(f'Error creating {model} in Odoo: {str(error)}')
            raise error

    def update_record(self, model, id, data):
        """Update a record"""
        try:
            result = self.models.execute_kw(
                self.db, self.uid, self.api_key,
                model, 'write',
                [[id], data]
            )
            return result
        except Exception as error:
            _logger.error(f'Error updating {model} in Odoo: {str(error)}')
            raise error
        
    def load_records(self, model, fields, data_rows):
        """Load records in bulk using the `load()` method.

        Args:
            model (str): The Odoo model name (e.g., 'res.partner').
            fields (list): List of field names (column headers).
            data_rows (list): List of lists containing record values.

        Returns:
            dict: Result from the `load()` call, typically includes created record IDs and any errors.
        """
        try:
            result = self.models.execute_kw(
                self.db, self.uid, self.api_key,
                model, 'load',
                [fields, data_rows]
            )
            return result
        except Exception as error:
            _logger.error(f'Error using load() for {model}: {str(error)}', exc_info=True)
            raise error

    def customer_exists(self, email):
        """Check if customer exists by email"""
        try:
            partners = self.search_read(
                'res.partner',
                [['email', '=', email]],
                ['id', 'name', 'email']
            )
            return partners[0] if partners else None
        except Exception as error:
            _logger.error(f'Error checking if customer exists: {str(error)}')
            raise error

    def company_exists(self, company_name):
        """Check if company exists by name"""
        try:
            companies = self.search_read(
                'res.partner',
                [
                    ['name', '=', company_name],
                    ['is_company', '=', True]
                ],
                ['id', 'name']
            )
            return companies[0] if companies else None
        except Exception as error:
            _logger.error(f'Error checking if company exists: {str(error)}')
            raise error

    
    def prefetch_states(self, force_refresh=False) -> Dict:
        """
        Prefetch all states from Odoo and create a map for faster lookups
        Returns a nested dictionary with structure:
        {
            country_id: {
                'code_map': {state_code: state_id, ...},
                'name_map': {state_name.lower(): state_id, ...}
            },
            ...
        }
        """
        # Return cached map if available and not forcing refresh
        if self._state_map is not None and not force_refresh:
            return self._state_map
            
        # Try to load from JSON file first
        if not force_refresh and os.path.exists(self._state_map_path):
            try:
                with open(self._state_map_path, 'r') as f:
                    self._state_map = json.load(f)
                _logger.info(f"Loaded state map from {self._state_map_path}")
                return self._state_map
            except Exception as e:
                _logger.warning(f"Failed to load state map from file: {str(e)}")
        
        # If file doesn't exist or force_refresh, fetch from Odoo
        try:
            _logger.info("Prefetching all states from Odoo...")
            states = self.search_read(
                'res.country.state',
                [],
                ['id', 'name', 'code', 'country_id']
            )
            
            # Build the state map
            state_map = {}
            for state in states:
                country_id = state['country_id'][0] if isinstance(state['country_id'], list) else state['country_id']
                
                # Initialize country entry if not exists
                if country_id not in state_map:
                    state_map[country_id] = {
                        'code_map': {},
                        'name_map': {}
                    }
                
                # Add to code map (case insensitive)
                if state['code']:
                    state_map[country_id]['code_map'][state['code'].lower()] = state['id']
                
                # Add to name map (case insensitive)
                if state['name']:
                    state_map[country_id]['name_map'][state['name'].lower()] = state['id']
            
            # Cache the map
            self._state_map = state_map
            
            # Save to JSON file for future use
            try:
                with open(self._state_map_path, 'w') as f:
                    json.dump(state_map, f)
                _logger.info(f"Saved state map to {self._state_map_path}")
            except Exception as e:
                _logger.warning(f"Failed to save state map to file: {str(e)}")
                
            return state_map
            
        except Exception as error:
            _logger.error(f'Error prefetching states: {str(error)}')
            return {}

    def get_state_id(self, country_id: int, province_code: str = None, state_name: str = None, create_if_not_found: bool = False) -> Optional[int]:
        """Get state ID by code or name using the prefetched state map"""
        if not country_id or (not province_code and not state_name):
            return None
        
        try:
            # Ensure we have the state map loaded
            state_map = self.prefetch_states()
            
            # Check if we have this country in our map
            if str(country_id) not in state_map and country_id not in state_map:
                _logger.warning(f"Country ID {country_id} not found in state map")
                return None
                
            # Convert country_id to string for JSON compatibility
            country_key = str(country_id)
            if country_id in state_map:
                country_key = country_id
                
            # Prefer search by code
            if province_code:
                code_lower = province_code.lower()
                if code_lower in state_map[country_key]['code_map']:
                    return state_map[country_key]['code_map'][code_lower]
            
            # Then fallback to search by name
            if state_name:
                name_lower = state_name.lower()
                if name_lower in state_map[country_key]['name_map']:
                    return state_map[country_key]['name_map'][name_lower]
            
            # If not found in our map, use the fallback method
            return None
            
        except Exception as error:
            _logger.error(f'Error in state lookup: {str(error)}')
            return None

    def find_or_create_attribute(self, attribute_name):
        """Find or create product attribute"""
        try:
            # Check if attribute exists
            attributes = self.search_read(
                'product.attribute',
                [['name', '=', attribute_name]],
                ['id']
            )
            
            if attributes:
                return attributes[0]['id']
            
            # Create new attribute
            return self.create_record('product.attribute', {
                'name': attribute_name,
                'create_variant': 'always',
                'display_type': 'select'
            })
        except Exception as error:
            _logger.error(f'Error finding or creating attribute {attribute_name}: {str(error)}')
            raise error

    def find_or_create_attribute_value(self, attribute_id, value_name):
        """Find or create product attribute value"""
        try:
            # Check if attribute value exists
            values = self.search_read(
                'product.attribute.value',
                [
                    ['attribute_id', '=', attribute_id],
                    ['name', '=', value_name]
                ],
                ['id']
            )
            
            if values:
                return values[0]['id']
            
            # Create new attribute value
            return self.create_record('product.attribute.value', {
                'name': value_name,
                'attribute_id': attribute_id
            })
        except Exception as error:
            _logger.error(f'Error finding or creating attribute value {value_name}: {str(error)}')
            raise error

    def find_attribute_value_id(self, attribute_id, value_name):
        """Find attribute value ID"""
        try:
            values = self.search_read(
                'product.attribute.value',
                [
                    ['attribute_id', '=', attribute_id],
                    ['name', '=', value_name]
                ],
                ['id']
            )
            
            return values[0]['id'] if values else None
        except Exception as error:
            _logger.error(f'Error finding attribute value {value_name}: {str(error)}')
            raise error

    def get_attribute_name(self, attribute_id):
        """Get attribute name by ID"""
        try:
            attributes = self.search_read(
                'product.attribute',
                [['id', '=', attribute_id]],
                ['name']
            )
            
            return attributes[0]['name'] if attributes else None
        except Exception as error:
            _logger.error(f'Error getting attribute name for ID {attribute_id}: {str(error)}')
            raise error

    def cleanup_product_data(self, template_id):
        """Clean up product data for template"""
        try:
            _logger.info(f'Cleaning up product data for template {template_id}')
            
            # First, check for existing variants
            variants = self.search_read(
                'product.product',
                [['product_tmpl_id', '=', template_id]],
                ['id', 'active']
            )
            
            if variants and len(variants) > 1:
                # Keep the first variant, which is usually the "main" one
                variant_ids = [v['id'] for v in variants[1:]]
                _logger.info(f'Found {len(variant_ids)} extra variants to remove')
                
                if variant_ids:
                    # Only archive variants, don't try to delete them
                    for variant_id in variant_ids:
                        try:
                            self.update_record('product.product', variant_id, {'active': False})
                            _logger.info(f'Archived variant {variant_id}')
                        except Exception as error:
                            _logger.error(f'Failed to archive variant {variant_id}: {str(error)}')
            
            return True
        except Exception as error:
            _logger.error(f'Error cleaning up product data for template {template_id}: {str(error)}')
            return False

    def order_exists(self, reference):
        """Check if order exists by reference number"""
        try:
            orders = self.search_read(
                'sale.order',
                [['client_order_ref', '=', reference]],
                ['id', 'name', 'client_order_ref']
            )
            return orders[0] if orders else None
        except Exception as error:
            _logger.error(f'Error checking if order exists: {str(error)}')
            raise error
    
    def get_title_id(self, title_shortcut: str) -> Optional[int]:
        """
        Get title ID by shortcut
        """
        if not title_shortcut:
            return None
        
        title_map = {"Ms.": "Miss"}
        normalized_title = title_map.get(title_shortcut, title_shortcut)
        
        titles = self.search_read(
            'res.partner.title', 
            [('shortcut', 'ilike', f'%{normalized_title}%')], 
            ['id'], 
            limit=1
        )
        return titles[0]['id'] if titles else None
    
    def get_industry_id(self, industry_name: str) -> Optional[int]:
        """
        Get or create industry ID
        """
        if not industry_name:
            return None
        
        # Search for existing industry
        industries = self.search_read(
            'res.partner.industry',
            [('name', 'ilike', industry_name)],
            ['id'],
            limit=1
        )
        if industries:
            return industries[0]['id']
        
        # Create new industry
        return self.create_record('res.partner.industry', {
            'name': industry_name,
            'active': True
        })

    def get_source_id(self, source_name: str) -> Optional[int]:
        """
        Get or create UTM source
        """
        if not source_name:
            return None
        
        # Search for existing source
        sources = self.search_read(
            'utm.source',
            [('name', '=', source_name)],
            ['id'],
            limit=1
        )
        
        if sources:
            return sources[0]['id']
        
        # Create new source
        return self.create_record('utm.source', {
            'name': source_name
        })
