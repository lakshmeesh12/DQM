from pydantic import BaseModel
from typing import Dict, List, Optional
import json
import os
import logging

logger = logging.getLogger(__name__)

class RuleUpdate(BaseModel):
    rules: List[str]
    type: str
    dq_dimensions: List[str]
    statistics: Optional[Dict] = None

class RuleAdd(BaseModel):
    column_name: str
    rules: List[str]
    type: str
    dq_dimensions: List[str]
    statistics: Optional[Dict] = None

class SingleRuleUpdate(BaseModel):
    rule: str

def load_rules(file_rules_dir: str, column_name: str) -> Dict:
    """Load rules for a specific column from the rules directory."""
    rule_file_path = os.path.join(file_rules_dir, f'{column_name}_rules.json')
    try:
        with open(rule_file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"rules": [], "type": "text", "dq_dimensions": ["Validity", "Completeness", "Relevance"], "statistics": None}

def save_rules(file_rules_dir: str, column_name: str, rules: Dict) -> None:
    """Save rules for a specific column to the rules directory."""
    rule_file_path = os.path.join(file_rules_dir, f'{column_name}_rules.json')
    os.makedirs(file_rules_dir, exist_ok=True)  # Ensure directory exists
    with open(rule_file_path, 'w') as f:
        json.dump(rules, f, indent=2)
    logger.info(f"Rules saved to {rule_file_path}")

def add_rule(file_rules_dir: str, column_name: str, rule: str) -> Dict:
    """Add a single rule to existing rules for a column."""
    current_rules = load_rules(file_rules_dir, column_name)
    if rule not in current_rules["rules"]:
        current_rules["rules"].append(rule)
        save_rules(file_rules_dir, column_name, current_rules)
        logger.info(f"Added new rule for {column_name}")
        return current_rules
    raise ValueError("Rule already exists")

def delete_rule(file_rules_dir: str, column_name: str, rule_index: int) -> Dict:
    """Delete a single rule from existing rules for a column."""
    current_rules = load_rules(file_rules_dir, column_name)
    if rule_index < 0 or rule_index >= len(current_rules["rules"]):
        raise ValueError("Invalid rule index")
    deleted_rule = current_rules["rules"].pop(rule_index)
    save_rules(file_rules_dir, column_name, current_rules)
    logger.info(f"Deleted rule at index {rule_index} for {column_name}")
    return {"deleted_rule": deleted_rule, "rules": current_rules}