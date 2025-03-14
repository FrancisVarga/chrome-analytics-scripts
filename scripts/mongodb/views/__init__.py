"""
MongoDB view creation utilities.

This module contains functions for creating views on MongoDB collections.
"""

from scripts.mongodb.views.conversation_analytics_report_view import (
    create_conversation_with_analytics_report_view
)
from scripts.mongodb.views.conversation_metrics import (
    create_daily_conversation_metrics_view,
    create_weekly_conversation_metrics_view,
    create_monthly_conversation_metrics_view,
    create_conversation_length_distribution_view
)
from scripts.mongodb.views.model_metrics import (
    create_model_usage_metrics_view,
    create_token_usage_metrics_view
)
from scripts.mongodb.views.user_metrics import (
    create_user_activity_metrics_view
)
from scripts.mongodb.views.category_metrics import (
    create_category_distribution_view
)
from scripts.mongodb.views.agent_metrics import (
    create_agent_mention_metrics_view,
    create_agent_mention_distribution_view
)
from scripts.mongodb.views.user_session_metrics import (
    create_user_session_metrics_view
)
from scripts.mongodb.views.transaction_metrics import (
    create_deposit_metrics_view,
    create_payment_method_metrics_view,
    create_bank_usage_metrics_view
)
from scripts.mongodb.views.conversation_input_metrics import (
    create_conversation_input_metrics_view,
    create_conversation_input_field_distribution_view,
    create_conversation_input_type_metrics_view,
    create_conversation_input_list_metrics_view,
    create_conversation_input_grouping_metrics_view,
    create_conversation_count_by_input_fields_view,
    create_daily_conversations_and_messages_by_input_fields_view
)

# List of all view creation functions
VIEW_CREATORS = [
    create_daily_conversation_metrics_view,
    create_weekly_conversation_metrics_view,
    create_monthly_conversation_metrics_view,
    create_model_usage_metrics_view,
    create_user_activity_metrics_view,
    create_category_distribution_view,
    create_conversation_length_distribution_view,
    create_token_usage_metrics_view,
    create_agent_mention_metrics_view,
    create_agent_mention_distribution_view,
    create_conversation_with_analytics_report_view,
    # New views
    create_user_session_metrics_view,
    create_deposit_metrics_view,
    create_payment_method_metrics_view,
    create_bank_usage_metrics_view,
    # Conversation input metrics views
    create_conversation_input_metrics_view,
    create_conversation_input_field_distribution_view,
    create_conversation_input_type_metrics_view,
    create_conversation_input_list_metrics_view,
    create_conversation_input_grouping_metrics_view,
    create_conversation_count_by_input_fields_view,
    create_daily_conversations_and_messages_by_input_fields_view
]

__all__ = [
    "create_daily_conversation_metrics_view",
    "create_weekly_conversation_metrics_view",
    "create_monthly_conversation_metrics_view",
    "create_model_usage_metrics_view",
    "create_user_activity_metrics_view",
    "create_category_distribution_view",
    "create_conversation_length_distribution_view",
    "create_token_usage_metrics_view",
    "create_agent_mention_metrics_view",
    "create_agent_mention_distribution_view",
    "create_conversation_with_analytics_report_view",
    "create_user_session_metrics_view",
    "create_deposit_metrics_view",
    "create_payment_method_metrics_view", 
    "create_bank_usage_metrics_view",
    "create_conversation_input_metrics_view",
    "create_conversation_input_field_distribution_view",
    "create_conversation_input_type_metrics_view",
    "create_conversation_input_list_metrics_view",
    "create_conversation_input_grouping_metrics_view",
    "create_conversation_count_by_input_fields_view",
    "create_daily_conversations_and_messages_by_input_fields_view",
    "VIEW_CREATORS"
]
