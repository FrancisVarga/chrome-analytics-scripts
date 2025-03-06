"""MongoDB schema definitions for the analytics framework."""

from typing import Dict, List, Any, Optional
from datetime import datetime

# MongoDB Schema Definitions

# These schema definitions are used for documentation and validation purposes.
# They define the structure of the documents stored in MongoDB collections.

CONVERSATION_ANALYTICS_SCHEMA = {
    "bsonType": "object",
    "required": ["_id", "app_id", "model_provider", "model_id", "created_at"],
    "properties": {
        "_id": {
            "bsonType": "string",
            "description": "Unique identifier for the conversation"
        },
        "app_id": {
            "bsonType": "string",
            "description": "Identifier for the application"
        },
        "model_provider": {
            "bsonType": "string",
            "description": "Provider of the model (e.g., 'openai', 'anthropic')"
        },
        "model_id": {
            "bsonType": "string",
            "description": "Identifier for the model (e.g., 'gpt-4', 'claude-2')"
        },
        "mode": {
            "bsonType": ["string", "null"],
            "description": "Conversation mode"
        },
        "name": {
            "bsonType": ["string", "null"],
            "description": "Name or title of the conversation"
        },
        "summary": {
            "bsonType": ["string", "null"],
            "description": "Summary of the conversation"
        },
        "from_end_user_id": {
            "bsonType": ["string", "null"],
            "description": "Identifier for the end user"
        },
        "from_account_id": {
            "bsonType": ["string", "null"],
            "description": "Identifier for the account"
        },
        "status": {
            "bsonType": "string",
            "description": "Status of the conversation (e.g., 'active', 'completed')"
        },
        "created_at": {
            "bsonType": "string",
            "description": "Timestamp when the conversation was created"
        },
        "updated_at": {
            "bsonType": "string",
            "description": "Timestamp when the conversation was last updated"
        },
        "is_deleted": {
            "bsonType": "bool",
            "description": "Whether the conversation is deleted"
        },
        "message_count": {
            "bsonType": "int",
            "description": "Number of messages in the conversation"
        },
        "total_tokens": {
            "bsonType": "int",
            "description": "Total number of tokens used in the conversation"
        },
        "total_price": {
            "bsonType": "double",
            "description": "Total price of the conversation"
        },
        "currency": {
            "bsonType": "string",
            "description": "Currency for the price (e.g., 'USD')"
        },
        "system_instruction": {
            "bsonType": ["string", "null"],
            "description": "System instructions for the conversation"
        },
        "system_instruction_tokens": {
            "bsonType": "int",
            "description": "Number of tokens in the system instruction"
        },
        "analytics_metadata": {
            "bsonType": "object",
            "description": "Additional metadata for analytics"
        },
        "messages": {
            "bsonType": "array",
            "description": "Messages in the conversation",
            "items": {
                "bsonType": "object",
                "required": ["message_id", "sequence_number", "role", "content"],
                "properties": {
                    "message_id": {
                        "bsonType": "string",
                        "description": "Unique identifier for the message"
                    },
                    "sequence_number": {
                        "bsonType": "int",
                        "description": "Sequence number of the message in the conversation"
                    },
                    "role": {
                        "bsonType": "string",
                        "description": "Role of the message sender (e.g., 'user', 'assistant', 'system')"
                    },
                    "content": {
                        "bsonType": "string",
                        "description": "Content of the message"
                    },
                    "tokens": {
                        "bsonType": "int",
                        "description": "Number of tokens in the message"
                    },
                    "price": {
                        "bsonType": "double",
                        "description": "Price of the message"
                    },
                    "created_at": {
                        "bsonType": "string",
                        "description": "Timestamp when the message was created"
                    },
                    "model_id": {
                        "bsonType": ["string", "null"],
                        "description": "Model used for this message if different from conversation"
                    },
                    "parent_message_id": {
                        "bsonType": ["string", "null"],
                        "description": "Identifier for the parent message in threaded conversations"
                    },
                    "metadata": {
                        "bsonType": "object",
                        "description": "Additional metadata for the message"
                    }
                }
            }
        },
        "categories": {
            "bsonType": "array",
            "description": "Categories assigned to the conversation",
            "items": {
                "bsonType": "object",
                "required": ["category_id", "category_type", "category_value"],
                "properties": {
                    "category_id": {
                        "bsonType": "string",
                        "description": "Unique identifier for the category"
                    },
                    "category_type": {
                        "bsonType": "string",
                        "description": "Type of category (e.g., 'topic', 'intent', 'sentiment')"
                    },
                    "category_value": {
                        "bsonType": "string",
                        "description": "Value of the category"
                    },
                    "confidence_score": {
                        "bsonType": "double",
                        "description": "Confidence score for the category assignment"
                    },
                    "created_at": {
                        "bsonType": "string",
                        "description": "Timestamp when the category was assigned"
                    },
                    "created_by": {
                        "bsonType": "string",
                        "description": "Entity that assigned the category (e.g., 'system', 'user')"
                    }
                }
            }
        }
    }
}

CONVERSATION_MESSAGES_SCHEMA = {
    "bsonType": "object",
    "required": ["_id", "conversation_id", "sequence_number", "role", "content"],
    "properties": {
        "_id": {
            "bsonType": "string",
            "description": "Unique identifier for the message"
        },
        "conversation_id": {
            "bsonType": "string",
            "description": "Identifier for the conversation"
        },
        "sequence_number": {
            "bsonType": "int",
            "description": "Sequence number of the message in the conversation"
        },
        "role": {
            "bsonType": "string",
            "description": "Role of the message sender (e.g., 'user', 'assistant', 'system')"
        },
        "content": {
            "bsonType": "string",
            "description": "Content of the message"
        },
        "tokens": {
            "bsonType": "int",
            "description": "Number of tokens in the message"
        },
        "price": {
            "bsonType": "double",
            "description": "Price of the message"
        },
        "currency": {
            "bsonType": "string",
            "description": "Currency for the price (e.g., 'USD')"
        },
        "created_at": {
            "bsonType": "string",
            "description": "Timestamp when the message was created"
        },
        "model_id": {
            "bsonType": ["string", "null"],
            "description": "Model used for this message if different from conversation"
        },
        "parent_message_id": {
            "bsonType": ["string", "null"],
            "description": "Identifier for the parent message in threaded conversations"
        },
        "metadata": {
            "bsonType": "object",
            "description": "Additional metadata for the message"
        }
    }
}

CONVERSATION_CATEGORIES_SCHEMA = {
    "bsonType": "object",
    "required": ["_id", "conversation_id", "category_type", "category_value"],
    "properties": {
        "_id": {
            "bsonType": "string",
            "description": "Unique identifier for the category"
        },
        "conversation_id": {
            "bsonType": "string",
            "description": "Identifier for the conversation"
        },
        "category_type": {
            "bsonType": "string",
            "description": "Type of category (e.g., 'topic', 'intent', 'sentiment')"
        },
        "category_value": {
            "bsonType": "string",
            "description": "Value of the category"
        },
        "confidence_score": {
            "bsonType": "double",
            "description": "Confidence score for the category assignment"
        },
        "created_at": {
            "bsonType": "string",
            "description": "Timestamp when the category was assigned"
        },
        "created_by": {
            "bsonType": "string",
            "description": "Entity that assigned the category (e.g., 'system', 'user')"
        }
    }
}

CONVERSATION_TRANSLATIONS_SCHEMA = {
    "bsonType": "object",
    "required": ["_id", "conversation_id", "language_code", "translated_content"],
    "properties": {
        "_id": {
            "bsonType": "string",
            "description": "Unique identifier for the translation"
        },
        "conversation_id": {
            "bsonType": "string",
            "description": "Identifier for the conversation"
        },
        "message_id": {
            "bsonType": ["string", "null"],
            "description": "Identifier for the message (null for conversation-level translations)"
        },
        "language_code": {
            "bsonType": "string",
            "description": "Language code (e.g., 'en', 'es', 'fr')"
        },
        "translated_content": {
            "bsonType": "string",
            "description": "Translated content"
        },
        "created_at": {
            "bsonType": "string",
            "description": "Timestamp when the translation was created"
        },
        "updated_at": {
            "bsonType": "string",
            "description": "Timestamp when the translation was last updated"
        }
    }
}

ANALYTICS_REPORTS_SCHEMA = {
    "bsonType": "object",
    "required": ["_id", "report_type", "period_start", "period_end", "report_data"],
    "properties": {
        "_id": {
            "bsonType": "string",
            "description": "Unique identifier for the report"
        },
        "report_type": {
            "bsonType": "string",
            "description": "Type of report (e.g., 'daily', 'weekly', 'monthly')"
        },
        "period_start": {
            "bsonType": "string",
            "description": "Start of the reporting period"
        },
        "period_end": {
            "bsonType": "string",
            "description": "End of the reporting period"
        },
        "report_data": {
            "bsonType": "object",
            "description": "Report data"
        },
        "created_at": {
            "bsonType": "string",
            "description": "Timestamp when the report was created"
        }
    }
}

USER_ANALYTICS_SCHEMA = {
    "bsonType": "object",
    "required": ["_id", "updated_at"],
    "properties": {
        "_id": {
            "bsonType": "string",
            "description": "User ID"
        },
        "updated_at": {
            "bsonType": "string",
            "description": "Timestamp when the analytics were last updated"
        },
        "total_conversations": {
            "bsonType": "int",
            "description": "Total number of conversations"
        },
        "total_messages": {
            "bsonType": "int",
            "description": "Total number of messages"
        },
        "total_tokens": {
            "bsonType": "int",
            "description": "Total number of tokens"
        },
        "total_price": {
            "bsonType": "double",
            "description": "Total price"
        },
        "first_conversation_at": {
            "bsonType": ["string", "null"],
            "description": "Timestamp of the first conversation"
        },
        "last_conversation_at": {
            "bsonType": ["string", "null"],
            "description": "Timestamp of the last conversation"
        },
        "daily_metrics": {
            "bsonType": "object",
            "description": "Metrics by day"
        },
        "category_distribution": {
            "bsonType": "object",
            "description": "Distribution of categories"
        },
        "model_usage": {
            "bsonType": "object",
            "description": "Usage by model"
        }
    }
}

# MongoDB Indexes

CONVERSATION_ANALYTICS_INDEXES = [
    {"key": {"from_end_user_id": 1, "created_at": -1}},
    {"key": {"app_id": 1, "created_at": -1}},
    {"key": {"model_id": 1, "created_at": -1}},
    {"key": {"created_at": 1}},
    {"key": {"status": 1, "created_at": -1}},
    {"key": {"categories.category_type": 1, "categories.category_value": 1}},
    {"key": {"total_tokens": 1}},
    {"key": {"total_price": 1}},
    # Compound indexes for common queries
    {"key": {"app_id": 1, "model_id": 1, "created_at": -1}},
    {"key": {"from_end_user_id": 1, "status": 1, "created_at": -1}},
    {"key": {"categories.category_type": 1, "categories.category_value": 1, "created_at": -1}},
    {"key": {"model_id": 1, "total_tokens": 1, "total_price": 1}}
]

CONVERSATION_MESSAGES_INDEXES = [
    {"key": {"conversation_id": 1}},
    {"key": {"conversation_id": 1, "sequence_number": 1}},
    {"key": {"parent_message_id": 1}}
]

CONVERSATION_CATEGORIES_INDEXES = [
    {"key": {"conversation_id": 1}},
    {"key": {"category_type": 1, "category_value": 1}}
]

CONVERSATION_TRANSLATIONS_INDEXES = [
    {"key": {"conversation_id": 1}},
    {"key": {"message_id": 1}},
    {"key": {"language_code": 1, "conversation_id": 1}}
]

ANALYTICS_REPORTS_INDEXES = [
    {"key": {"report_type": 1, "period_start": 1, "period_end": 1}},
    {"key": {"created_at": 1}}
]

USER_ANALYTICS_INDEXES = [
    {"key": {"total_conversations": 1}},
    {"key": {"last_conversation_at": 1}}
]

# Python Data Models

class ConversationAnalytics:
    """Data model for conversation analytics."""
    
    def __init__(
        self,
        id: str,
        app_id: str,
        model_provider: str,
        model_id: str,
        mode: Optional[str] = None,
        name: Optional[str] = None,
        summary: Optional[str] = None,
        from_end_user_id: Optional[str] = None,
        from_account_id: Optional[str] = None,
        status: str = "active",
        created_at: Optional[str] = None,
        updated_at: Optional[str] = None,
        is_deleted: bool = False,
        message_count: int = 0,
        total_tokens: int = 0,
        total_price: float = 0.0,
        currency: str = "USD",
        system_instruction: Optional[str] = None,
        system_instruction_tokens: int = 0,
        analytics_metadata: Optional[Dict[str, Any]] = None,
        messages: Optional[List[Dict[str, Any]]] = None,
        categories: Optional[List[Dict[str, Any]]] = None
    ):
        """
        Initialize a conversation analytics object.
        
        Args:
            id: Unique identifier for the conversation
            app_id: Identifier for the application
            model_provider: Provider of the model
            model_id: Identifier for the model
            mode: Conversation mode
            name: Name or title of the conversation
            summary: Summary of the conversation
            from_end_user_id: Identifier for the end user
            from_account_id: Identifier for the account
            status: Status of the conversation
            created_at: Timestamp when the conversation was created
            updated_at: Timestamp when the conversation was last updated
            is_deleted: Whether the conversation is deleted
            message_count: Number of messages in the conversation
            total_tokens: Total number of tokens used in the conversation
            total_price: Total price of the conversation
            currency: Currency for the price
            system_instruction: System instructions for the conversation
            system_instruction_tokens: Number of tokens in the system instruction
            analytics_metadata: Additional metadata for analytics
            messages: Messages in the conversation
            categories: Categories assigned to the conversation
        """
        self.id = id
        self.app_id = app_id
        self.model_provider = model_provider
        self.model_id = model_id
        self.mode = mode
        self.name = name
        self.summary = summary
        self.from_end_user_id = from_end_user_id
        self.from_account_id = from_account_id
        self.status = status
        self.created_at = created_at or datetime.now().isoformat()
        self.updated_at = updated_at or datetime.now().isoformat()
        self.is_deleted = is_deleted
        self.message_count = message_count
        self.total_tokens = total_tokens
        self.total_price = total_price
        self.currency = currency
        self.system_instruction = system_instruction
        self.system_instruction_tokens = system_instruction_tokens
        self.analytics_metadata = analytics_metadata or {}
        self.messages = messages or []
        self.categories = categories or []
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for MongoDB.
        
        Returns:
            Dictionary representation
        """
        return {
            "_id": self.id,
            "app_id": self.app_id,
            "model_provider": self.model_provider,
            "model_id": self.model_id,
            "mode": self.mode,
            "name": self.name,
            "summary": self.summary,
            "from_end_user_id": self.from_end_user_id,
            "from_account_id": self.from_account_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "is_deleted": self.is_deleted,
            "message_count": self.message_count,
            "total_tokens": self.total_tokens,
            "total_price": self.total_price,
            "currency": self.currency,
            "system_instruction": self.system_instruction,
            "system_instruction_tokens": self.system_instruction_tokens,
            "analytics_metadata": self.analytics_metadata,
            "messages": self.messages,
            "categories": self.categories
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConversationAnalytics':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary representation
            
        Returns:
            ConversationAnalytics object
        """
        # Handle _id vs id
        id_value = data.get("_id") or data.get("id")
        
        return cls(
            id=id_value,
            app_id=data.get("app_id"),
            model_provider=data.get("model_provider"),
            model_id=data.get("model_id"),
            mode=data.get("mode"),
            name=data.get("name"),
            summary=data.get("summary"),
            from_end_user_id=data.get("from_end_user_id"),
            from_account_id=data.get("from_account_id"),
            status=data.get("status", "active"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            is_deleted=data.get("is_deleted", False),
            message_count=data.get("message_count", 0),
            total_tokens=data.get("total_tokens", 0),
            total_price=data.get("total_price", 0.0),
            currency=data.get("currency", "USD"),
            system_instruction=data.get("system_instruction"),
            system_instruction_tokens=data.get("system_instruction_tokens", 0),
            analytics_metadata=data.get("analytics_metadata", {}),
            messages=data.get("messages", []),
            categories=data.get("categories", [])
        )


class ConversationMessage:
    """Data model for conversation messages."""
    
    def __init__(
        self,
        id: str,
        conversation_id: str,
        sequence_number: int,
        role: str,
        content: str,
        tokens: int = 0,
        price: float = 0.0,
        currency: str = "USD",
        created_at: Optional[str] = None,
        model_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize a conversation message object.
        
        Args:
            id: Unique identifier for the message
            conversation_id: Identifier for the conversation
            sequence_number: Sequence number of the message in the conversation
            role: Role of the message sender
            content: Content of the message
            tokens: Number of tokens in the message
            price: Price of the message
            currency: Currency for the price
            created_at: Timestamp when the message was created
            model_id: Model used for this message if different from conversation
            parent_message_id: Identifier for the parent message in threaded conversations
            metadata: Additional metadata for the message
        """
        self.id = id
        self.conversation_id = conversation_id
        self.sequence_number = sequence_number
        self.role = role
        self.content = content
        self.tokens = tokens
        self.price = price
        self.currency = currency
        self.created_at = created_at or datetime.now().isoformat()
        self.model_id = model_id
        self.parent_message_id = parent_message_id
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for MongoDB.
        
        Returns:
            Dictionary representation
        """
        return {
            "_id": self.id,
            "conversation_id": self.conversation_id,
            "sequence_number": self.sequence_number,
            "role": self.role,
            "content": self.content,
            "tokens": self.tokens,
            "price": self.price,
            "currency": self.currency,
            "created_at": self.created_at,
            "model_id": self.model_id,
            "parent_message_id": self.parent_message_id,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConversationMessage':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary representation
            
        Returns:
            ConversationMessage object
        """
        # Handle _id vs id
        id_value = data.get("_id") or data.get("id")
        
        return cls(
            id=id_value,
            conversation_id=data.get("conversation_id"),
            sequence_number=data.get("sequence_number"),
            role=data.get("role"),
            content=data.get("content", ""),
            tokens=data.get("tokens", 0),
            price=data.get("price", 0.0),
            currency=data.get("currency", "USD"),
            created_at=data.get("created_at"),
            model_id=data.get("model_id"),
            parent_message_id=data.get("parent_message_id"),
            metadata=data.get("metadata", {})
        )


class ConversationCategory:
    """Data model for conversation categories."""
    
    def __init__(
        self,
        id: str,
        conversation_id: str,
        category_type: str,
        category_value: str,
        confidence_score: float = 1.0,
        created_at: Optional[str] = None,
        created_by: str = "system"
    ):
        """
        Initialize a conversation category object.
        
        Args:
            id: Unique identifier for the category
            conversation_id: Identifier for the conversation
            category_type: Type of category
            category_value: Value of the category
            confidence_score: Confidence score for the category assignment
            created_at: Timestamp when the category was assigned
            created_by: Entity that assigned the category
        """
        self.id = id
        self.conversation_id = conversation_id
        self.category_type = category_type
        self.category_value = category_value
        self.confidence_score = confidence_score
        self.created_at = created_at or datetime.now().isoformat()
        self.created_by = created_by
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for MongoDB.
        
        Returns:
            Dictionary representation
        """
        return {
            "_id": self.id,
            "conversation_id": self.conversation_id,
            "category_type": self.category_type,
            "category_value": self.category_value,
            "confidence_score": self.confidence_score,
            "created_at": self.created_at,
            "created_by": self.created_by
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConversationCategory':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary representation
            
        Returns:
            ConversationCategory object
        """
        # Handle _id vs id
        id_value = data.get("_id") or data.get("id")
        
        return cls(
            id=id_value,
            conversation_id=data.get("conversation_id"),
            category_type=data.get("category_type"),
            category_value=data.get("category_value"),
            confidence_score=data.get("confidence_score", 1.0),
            created_at=data.get("created_at"),
            created_by=data.get("created_by", "system")
        )


class ConversationTranslation:
    """Data model for conversation translations."""
    
    def __init__(
        self,
        id: str,
        conversation_id: str,
        language_code: str,
        translated_content: str,
        message_id: Optional[str] = None,
        created_at: Optional[str] = None,
        updated_at: Optional[str] = None
    ):
        """
        Initialize a conversation translation object.
        
        Args:
            id: Unique identifier for the translation
            conversation_id: Identifier for the conversation
            language_code: Language code
            translated_content: Translated content
            message_id: Identifier for the message (None for conversation-level translations)
            created_at: Timestamp when the translation was created
            updated_at: Timestamp when the translation was last updated
        """
        self.id = id
        self.conversation_id = conversation_id
        self.language_code = language_code
        self.translated_content = translated_content
        self.message_id = message_id
        self.created_at = created_at or datetime.now().isoformat()
        self.updated_at = updated_at or datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for MongoDB.
        
        Returns:
            Dictionary representation
        """
        return {
            "_id": self.id,
            "conversation_id": self.conversation_id,
            "language_code": self.language_code,
            "translated_content": self.translated_content,
            "message_id": self.message_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConversationTranslation':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary representation
            
        Returns:
            ConversationTranslation object
        """
        # Handle _id vs id
        id_value = data.get("_id") or data.get("id")
        
        return cls(
            id=id_value,
            conversation_id=data.get("conversation_id"),
            language_code=data.get("language_code"),
            translated_content=data.get("translated_content", ""),
            message_id=data.get("message_id"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at")
        )


class AnalyticsReport:
    """Data model for analytics reports."""
    
    def __init__(
        self,
        id: str,
        report_type: str,
        period_start: str,
        period_end: str,
        report_data: Dict[str, Any],
        created_at: Optional[str] = None
    ):
        """
        Initialize an analytics report object.
        
        Args:
            id: Unique identifier for the report
            report_type: Type of report
            period_start: Start of the reporting period
            period_end: End of the reporting period
            report_data: Report data
            created_at: Timestamp when the report was created
        """
        self.id = id
        self.report_type = report_type
        self.period_start = period_start
        self.period_end = period_end
        self.report_data = report_data
        self.created_at = created_at or datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for MongoDB.
        
        Returns:
            Dictionary representation
        """
        return {
            "_id": self.id,
            "report_type": self.report_type,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "report_data": self.report_data,
            "created_at": self.created_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AnalyticsReport':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary representation
            
        Returns:
            AnalyticsReport object
        """
        # Handle _id vs id
        id_value = data.get("_id") or data.get("id")
        
        return cls(
            id=id_value,
            report_type=data.get("report_type"),
            period_start=data.get("period_start"),
            period_end=data.get("period_end"),
            report_data=data.get("report_data", {}),
            created_at=data.get("created_at")
        )


class UserAnalytics:
    """Data model for user analytics."""
    
    def __init__(
        self,
        id: str,
        updated_at: Optional[str] = None,
        total_conversations: int = 0,
        total_messages: int = 0,
        total_tokens: int = 0,
        total_price: float = 0.0,
        first_conversation_at: Optional[str] = None,
        last_conversation_at: Optional[str] = None,
        daily_metrics: Optional[Dict[str, Dict[str, Any]]] = None,
        category_distribution: Optional[Dict[str, int]] = None,
        model_usage: Optional[Dict[str, Dict[str, Any]]] = None
    ):
        """
        Initialize a user analytics object.
        
        Args:
            id: User ID
            updated_at: Timestamp when the analytics were last updated
            total_conversations: Total number of conversations
            total_messages: Total number of messages
            total_tokens: Total number of tokens
            total_price: Total price
            first_conversation_at: Timestamp of the first conversation
            last_conversation_at: Timestamp of the last conversation
            daily_metrics: Metrics by day
            category_distribution: Distribution of categories
            model_usage: Usage by model
        """
        self.id = id
        self.updated_at = updated_at or datetime.now().isoformat()
        self.total_conversations = total_conversations
        self.total_messages = total_messages
        self.total_tokens = total_tokens
        self.total_price = total_price
        self.first_conversation_at = first_conversation_at
        self.last_conversation_at = last_conversation_at
        self.daily_metrics = daily_metrics or {}
        self.category_distribution = category_distribution or {}
        self.model_usage = model_usage or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for MongoDB.
        
        Returns:
            Dictionary representation
        """
        return {
            "_id": self.id,
            "updated_at": self.updated_at,
            "total_conversations": self.total_conversations,
            "total_messages": self.total_messages,
            "total_tokens": self.total_tokens,
            "total_price": self.total_price,
            "first_conversation_at": self.first_conversation_at,
            "last_conversation_at": self.last_conversation_at,
            "daily_metrics": self.daily_metrics,
            "category_distribution": self.category_distribution,
            "model_usage": self.model_usage
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserAnalytics':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary representation
            
        Returns:
            UserAnalytics object
        """
        # Handle _id vs id
        id_value = data.get("_id") or data.get("id")
        
        return cls(
            id=id_value,
            updated_at=data.get("updated_at"),
            total_conversations=data.get("total_conversations", 0),
            total_messages=data.get("total_messages", 0),
            total_tokens=data.get("total_tokens", 0),
            total_price=data.get("total_price", 0.0),
            first_conversation_at=data.get("first_conversation_at"),
            last_conversation_at=data.get("last_conversation_at"),
            daily_metrics=data.get("daily_metrics", {}),
            category_distribution=data.get("category_distribution", {}),
            model_usage=data.get("model_usage", {})
        )
