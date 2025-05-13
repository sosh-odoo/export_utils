from typing import Dict, Any, List, Optional, Tuple
import json
import os
from odoo.tools import config

class SalesforceHelper:    
    def get_stage_id(self, stage_name: str, odoo_api) -> Optional[int]:
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
    
    def get_industry_id(self, industry_name: str, odoo_api) -> Optional[int]:
        """
        Map Salesforce industry to Odoo industry and return its ID.
        If not found, fallback to 'Other Services'.
        """
        if not industry_name:
            industry_name = "Other"

        # Map Salesforce industry to Odoo industry
        industry_mapping = {
            "Agriculture": "Agriculture",
            "Apparel": "Apparel",
            "Banking": "Finance/Insurance",
            "Biotechnology": "Biotechnology",
            "Chemicals": "Manufacturing",
            "Communications": "IT/Communication",
            "Construction": "Construction",
            "Consulting": "Consulting",
            "Education": "Education",
            "Electronics": "Electronics",
            "Energy": "Energy supply",
            "Engineering": "Scientific",
            "Entertainment": "Entertainment",
            "Environmental": "Scientific",
            "Finance": "Finance/Insurance",
            "Food & Beverage": "Food/Hospitality",
            "Government": "Public Administration",
            "Healthcare": "Health/Social",
            "Hospitality": "Food/Hospitality",
            "Insurance": "Finance/Insurance",
            "Machinery": "Manufacturing",
            "Manufacturing": "Manufacturing",
            "Media": "Entertainment",
            "Not For Profit": "Other Services",
            "Recreation": "Entertainment",
            "Retail": "Wholesale/Retail",
            "Shipping": "Transportation/Logistics",
            "Technology": "IT/Communication",
            "Telecommunications": "IT/Communication",
            "Transportation": "Transportation/Logistics",
            "Utilities": "Administrative/Utilities",
            "Other": "Other Services",
        }

        odoo_industry_name = industry_mapping.get(industry_name, "Other Services")

        # Search for Industry by name
        industries = odoo_api.search_read(
            'res.partner.industry',
            [('name', '=', odoo_industry_name)],
            ['id'],
            limit=1
        )
        return industries[0]['id'] if industries else None

    def get_title_id(self, title_shortcut: str, odoo_api) -> Optional[int]:
        """
        Get title ID by shortcut
        """
        if not title_shortcut:
            return None
        
        title_map = {"Ms.": "Miss"}
        normalized_title = title_map.get(title_shortcut, title_shortcut)
        
        titles = odoo_api.search_read(
            'res.partner.title', 
            [('shortcut', 'ilike', f'%{normalized_title}%')], 
            ['id'], 
            limit=1
        )
        return titles[0]['id'] if titles else None