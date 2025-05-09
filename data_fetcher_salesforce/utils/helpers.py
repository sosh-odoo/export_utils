from typing import Dict, Any, List, Optional, Tuple
import json
import os
from odoo.tools import config
class SalesforceHelper:    
    def get_or_create_partner(self, odoo_api, data: Dict) -> Tuple[int, bool]:
        """
        Get existing partner or create new one
        """
        # Check if partner exists by salesforce_id
        if 'ref' in data:
            partners = odoo_api.search_read(
                'res.partner', 
                [('ref', '=', data['ref'])], 
                ['id'], 
                limit=1
            )
            if partners:
                return partners[0]['id'], False
        
        # Create new partner
        partner_id = odoo_api.create_record('res.partner', data)
        return partner_id, True
    
    def get_or_create_product(self, odoo_api, data: Dict) -> Tuple[int, bool]:
        """
        Get existing product or create new one
        """
        # Check if product exists by salesforce_id
        if 'sf_product_id' in data:
            products = odoo_api.search_read(
                'product.template', 
                [('description', '=', data['sf_product_id'])], 
                ['id','name'], 
                limit=1
            )
            if products:
                return products[0]['id'], products[0]['name'], False
        
        # Create new product
        product_id = odoo_api.create_record('product.template', data)
        return product_id, True
    
    def get_order_id(self, odoo_api, data: Dict) -> Tuple[int, bool]:
        """
        Get existing order id
        """
        if 'sf_order_id' in data:
            order = odoo_api.search_read(
                'sale.order', 
                [('reference', '=', data['sf_order_id'])], 
                ['id'], 
                limit=1
            )
            if order:
                return order[0]['id']
        return None
        
    def get_invoice_id(self, odoo_api, data: Dict) -> Tuple[int, bool]:
        """
        Get existing invoice id
        """
        if 'sf_invoice_id' in data:
            order = odoo_api.search_read(
                'account.move', 
                [('ref', '=', data['sf_invoice_id'])], 
                ['id'], 
                limit=1
            )
            if order:
                return order[0]['id']
        return None
    
    def get_stage_id(self, stage_name, odoo_api):
        """
        Map Salesforce stage to Odoo stage
        """
        # Map Salesforce stages to Odoo stages
        stage_mapping = {
            "Prospecting": "New",
            "Id. Decision Makers":"Id. Decision Makers",
            "Qualification": "Qualified",
            "Needs Analysis": "Needs Analysis",
            "Perception Analysis":"Perception Analysis",
            "Proposal/Price Quote": "Proposition",
            "Value Proposition" : "Proposition",
            "Negotiation/Review": "Negotiation",
            "Closed Won": "Won",
            "Closed Lost": "Lost",
            "Qualified": "Qualified",
            "New": "New",
        }
        
        odoo_stage_name = stage_mapping.get(stage_name, "New")
        
        # Search for stage by name
        stages = odoo_api.search_read(
            'crm.stage',
            [('name', '=', odoo_stage_name)],
            ['id'],
            limit=1
        )
        
        if stages:
            return stages[0]['id']

        # Create new stage
        return odoo_api.create_record('crm.stage', {
            'name': odoo_stage_name
        })
    
    def get_state_name_from_code(state_code, country_code=None):
        """
        Look up a state name from its code using the mapping file.
        
        Args:
            state_code (str): The state code to look up (e.g., 'CA')
            country_code (str, optional): Country code to restrict the search to a specific country
                                        (useful when state codes are duplicated across countries)
        
        Returns:
            str: The state name if found, or the original state code if not found
        """

        
        # Path to the mapping file (adjust as needed)
        module_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_file = os.path.join(module_dir, '..', 'data', 'odoo_state_mapping.json')
            
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                state_mapping = json.load(f)
                
            if country_code and country_code in state_mapping['by_country']:
                # Look up in the specific country
                country_states = state_mapping['by_country'][country_code]
                if state_code in country_states:
                    return country_states[state_code]
            
            # Fall back to the global mapping if not found or no country specified
            if state_code in state_mapping['all_states']:
                return state_mapping['all_states'][state_code]
                
            # Return the original code if not found
            return state_code
            
        except (IOError, json.JSONDecodeError, KeyError) as e:
            # Log error but don't fail import process
            import logging
            _logger = logging.getLogger(__name__)
            _logger.warning(f"Error looking up state name for code '{state_code}': {e}")
            return state_code