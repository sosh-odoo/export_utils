from typing import Dict, Any, List, Optional, Tuple
import json
import os
from odoo.tools import config
class SalesforceHelper:    
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
    