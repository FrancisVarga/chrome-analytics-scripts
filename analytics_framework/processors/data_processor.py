"""Process and transform data from NocoDB to MongoDB format."""

import logging
from datetime import datetime
from typing import Dict, List, Any
import re

from ..utils.thread_pool import thread_pool_manager


class DataProcessor:
    """Process and transform data from NocoDB to MongoDB format."""
    
    def __init__(self):
        """Initialize the data processor."""
        self.logger = logging.getLogger(__name__)
    
    def process_conversations_batch(
        self,
        conversations: List[Dict[str, Any]],
        messages_by_conversation: Dict[str, List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """
        Process a batch of conversations with their messages.
        
        Args:
            conversations: List of conversations
            messages_by_conversation: Dictionary mapping conversation IDs to messages
            
        Returns:
            List of processed conversation documents
        """
        # Use thread pool to process conversations in parallel
        return thread_pool_manager.map_processing(
            self.process_conversation_with_messages,
            conversations,
            messages_by_conversation
        )
    
    def process_conversation_with_messages(
        self,
        conversation: Dict[str, Any],
        messages_by_conversation: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """
        Process a conversation with its messages.
        
        Args:
            conversation: Conversation data
            messages_by_conversation: Dictionary mapping conversation IDs to messages
            
        Returns:
            Processed conversation document
        """
        conversation_id = conversation.get('id')
        messages = messages_by_conversation.get(conversation_id, [])
        
        # Sort messages by creation time or sequence
        sorted_messages = sorted(messages, key=lambda m: m.get('created_at', ''))
        
        # Calculate analytics metrics
        message_count = len(messages)
        total_tokens = sum(m.get('message_tokens', 0) + m.get('answer_tokens', 0) for m in messages)
        total_price = sum(m.get('total_price', 0) for m in messages)
        
        # Process messages into embedded format
        processed_messages = []
        for i, msg in enumerate(sorted_messages):
            processed_message = {
                "message_id": msg.get('id'),
                "sequence_number": i + 1,
                "role": self._determine_message_role(msg, i),
                "content": msg.get('query') if i % 2 == 0 else msg.get('answer'),
                "tokens": msg.get('message_tokens', 0) if i % 2 == 0 else msg.get('answer_tokens', 0),
                "price": msg.get('total_price', 0),
                "created_at": msg.get('created_at'),
                "model_id": msg.get('model_id'),
                "parent_message_id": msg.get('parent_message_id')
            }
            processed_messages.append(processed_message)
        
        # Create the MongoDB document
        mongo_doc = {
            "_id": conversation.get('id'),
            "app_id": conversation.get('app_id'),
            "model_provider": conversation.get('model_provider'),
            "model_id": conversation.get('model_id'),
            "mode": conversation.get('mode'),
            "name": conversation.get('name'),
            "summary": conversation.get('summary'),
            "from_end_user_id": conversation.get('from_end_user_id'),
            "from_account_id": conversation.get('from_account_id'),
            "status": conversation.get('status', 'active'),
            "created_at": conversation.get('created_at'),
            "updated_at": conversation.get('updated_at'),
            "is_deleted": conversation.get('is_deleted', False),
            
            # Analytics fields
            "message_count": message_count,
            "total_tokens": total_tokens,
            "total_price": total_price,
            "currency": messages[0].get('currency', 'USD') if messages else 'USD',
            "system_instruction": conversation.get('system_instruction'),
            "system_instruction_tokens": conversation.get('system_instruction_tokens', 0),
            
            # Embedded messages
            "messages": processed_messages,
            
            # Empty categories array (to be filled later)
            "categories": [],
            
            # Analytics metadata
            "analytics_metadata": {
                "processed_at": datetime.now().isoformat()
            }
        }
        
        return mongo_doc
    
    def _determine_message_role(self, message: Dict[str, Any], index: int) -> str:
        """
        Determine the role of a message.
        
        Args:
            message: Message data
            index: Message index in the conversation
            
        Returns:
            Role string ("user", "assistant", or "system")
        """
        # If the message has an explicit role, use it
        if 'role' in message:
            return message['role']
        
        # Otherwise infer from position and content
        if 'system_instruction' in message and message['system_instruction']:
            return 'system'
        
        # Assume alternating user/assistant pattern
        return 'user' if index % 2 == 0 else 'assistant'
    
    def extract_categories(
        self,
        conversation: Dict[str, Any],
        messages: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Extract categories for a conversation.
        
        Args:
            conversation: Conversation data
            messages: List of messages
            
        Returns:
            List of category objects
        """
        categories = []
        
        # Extract user messages for analysis
        user_messages = [msg for msg in messages if self._determine_message_role(msg, 0) == 'user']
        user_content = " ".join([msg.get('query', '') for msg in user_messages])
        
        # Topic categorization
        topic_categories = self._extract_topic_categories(user_content, conversation)
        categories.extend(topic_categories)
        
        # Intent categorization
        intent_categories = self._extract_intent_categories(user_content, conversation)
        categories.extend(intent_categories)
        
        # Sentiment analysis
        sentiment_categories = self._analyze_sentiment(user_content, conversation)
        categories.extend(sentiment_categories)
        
        return categories
    
    def _extract_topic_categories(
        self,
        content: str,
        conversation: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extract topic categories from content.
        
        Args:
            content: Text content to analyze
            conversation: Conversation data
            
        Returns:
            List of topic category objects
        """
        categories = []
        content_lower = content.lower()
        
        # Define topic keywords with weighted importance
        topic_keywords = {
            "pricing": {
                "primary": ["price", "cost", "pricing", "payment", "subscription", "billing", "fee"],
                "secondary": ["discount", "plan", "trial", "free", "premium", "upgrade", "downgrade", "charge"],
                "weight": 1.0
            },
            "technical_issue": {
                "primary": ["error", "bug", "issue", "problem", "crash", "not working", "broken", "fail"],
                "secondary": ["fix", "resolve", "solution", "troubleshoot", "debug", "repair", "malfunction", "glitch"],
                "weight": 1.0
            },
            "feature_request": {
                "primary": ["feature", "add", "implement", "enhancement", "improve", "suggestion"],
                "secondary": ["functionality", "capability", "option", "ability", "support for", "integration"],
                "weight": 0.9
            },
            "account": {
                "primary": ["account", "login", "password", "sign in", "sign up", "register", "profile"],
                "secondary": ["username", "email", "authentication", "credentials", "forgot", "reset", "verification"],
                "weight": 0.9
            },
            "general_inquiry": {
                "primary": ["how to", "what is", "explain", "help", "guide", "tutorial", "documentation"],
                "secondary": ["instructions", "steps", "process", "procedure", "information", "details", "clarify"],
                "weight": 0.8
            },
            "feedback": {
                "primary": ["feedback", "review", "opinion", "think", "suggest", "recommendation"],
                "secondary": ["impression", "experience", "satisfaction", "dissatisfaction", "rating", "evaluation"],
                "weight": 0.8
            },
            "data_privacy": {
                "primary": ["privacy", "data", "gdpr", "ccpa", "personal information", "consent", "opt-out"],
                "secondary": ["collect", "store", "share", "delete", "retention", "policy", "compliance"],
                "weight": 1.0
            },
            "integration": {
                "primary": ["integrate", "integration", "connect", "api", "webhook", "sync", "import", "export"],
                "secondary": ["third-party", "platform", "service", "tool", "compatibility", "connection"],
                "weight": 0.9
            }
        }
        
        # Context-aware scoring
        topic_scores = {}
        
        for topic, keyword_data in topic_keywords.items():
            # Calculate primary keyword matches (higher weight)
            primary_matches = [keyword for keyword in keyword_data["primary"] if keyword in content_lower]
            primary_count = sum(content_lower.count(keyword) for keyword in primary_matches)
            
            # Calculate secondary keyword matches (lower weight)
            secondary_matches = [keyword for keyword in keyword_data["secondary"] if keyword in content_lower]
            secondary_count = sum(content_lower.count(keyword) for keyword in secondary_matches)
            
            # Calculate weighted score
            topic_weight = keyword_data["weight"]
            score = (primary_count * 0.7 + secondary_count * 0.3) * topic_weight
            
            # Consider context from conversation metadata
            if conversation.get("name") and any(keyword in conversation["name"].lower() for keyword in keyword_data["primary"]):
                score += 0.5
                
            if score > 0:
                topic_scores[topic] = score
        
        # Normalize scores and create categories
        if topic_scores:
            max_score = max(topic_scores.values())
            
            for topic, score in topic_scores.items():
                # Calculate confidence (normalized score with minimum threshold)
                normalized_score = score / max_score
                confidence = min(max(0.6, 0.5 + (normalized_score * 0.4)), 0.95)
                
                categories.append({
                    "category_id": f"{conversation['id']}_topic_{topic}",
                    "category_type": "topic",
                    "category_value": topic,
                    "confidence_score": confidence,
                    "created_at": datetime.now().isoformat(),
                    "created_by": "system"
                })
        
        return categories
    
    def _extract_intent_categories(
        self,
        content: str,
        conversation: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extract intent categories from content.
        
        Args:
            content: Text content to analyze
            conversation: Conversation data
            
        Returns:
            List of intent category objects
        """
        categories = []
        content_lower = content.lower()
        
        # Define intent patterns with weights
        intent_patterns = {
            "question": {
                "patterns": [
                    r"\?$", 
                    r"^(what|how|why|when|where|who|can|could|would|will|is|are|do|does|did)",
                    r"(tell me|explain|describe|elaborate on|clarify)"
                ],
                "weight": 1.0
            },
            "request": {
                "patterns": [
                    r"^(please|can you|could you|would you|will you|i need|i want|i would like|i'd like)",
                    r"(help me|assist me|support me|guide me)",
                    r"(create|update|delete|modify|change|add|remove)"
                ],
                "weight": 0.9
            },
            "complaint": {
                "patterns": [
                    r"(not working|doesn't work|isn't working|broken|issue|problem|bug|error|crash)",
                    r"(disappointed|unhappy|frustrated|annoyed|upset|dissatisfied)",
                    r"(failed|failure|poor|bad|terrible|awful|horrible)"
                ],
                "weight": 1.0
            },
            "feedback": {
                "patterns": [
                    r"(feedback|review|opinion|think|suggest|improve|enhancement)",
                    r"(like|love|enjoy|appreciate|prefer)",
                    r"(don't like|dislike|hate|not a fan)"
                ],
                "weight": 0.9
            },
            "greeting": {
                "patterns": [
                    r"^(hi|hello|hey|greetings|good morning|good afternoon|good evening)",
                    r"(nice to meet you|pleasure to meet you)"
                ],
                "weight": 0.7
            },
            "gratitude": {
                "patterns": [
                    r"(thank|thanks|appreciate|grateful|much appreciated)",
                    r"(you've been helpful|you're the best|excellent service)"
                ],
                "weight": 0.8
            },
            "farewell": {
                "patterns": [
                    r"(bye|goodbye|see you|talk to you later|until next time)",
                    r"(have a good day|have a nice day|have a great day)"
                ],
                "weight": 0.7
            },
            "troubleshooting": {
                "patterns": [
                    r"(troubleshoot|diagnose|debug|fix|resolve|solve)",
                    r"(steps to|how to fix|how to resolve|how to solve)",
                    r"(tried|attempted|not working after)"
                ],
                "weight": 1.0
            },
            "purchase_intent": {
                "patterns": [
                    r"(buy|purchase|subscribe|order|get|acquire)",
                    r"(how much|price|cost|fee|subscription|payment)",
                    r"(discount|offer|deal|promotion|coupon)"
                ],
                "weight": 1.0
            }
        }
        
        # Context-aware scoring
        intent_scores = {}
        
        for intent, data in intent_patterns.items():
            patterns = data["patterns"]
            weight = data["weight"]
            
            # Count pattern matches
            match_count = sum(1 for pattern in patterns if re.search(pattern, content_lower))
            
            # Calculate weighted score
            if match_count > 0:
                # More sophisticated scoring that considers:
                # - Number of matches relative to total patterns
                # - Position of matches (beginning of message is more important)
                # - Length of content (shorter content with matches has higher relevance)
                pattern_coverage = match_count / len(patterns)
                position_factor = 1.0
                
                # Check if any pattern matches at the beginning
                if any(re.match(pattern, content_lower) for pattern in patterns):
                    position_factor = 1.2
                
                # Length factor (shorter messages with matches are more focused)
                length_factor = 1.0
                if len(content_lower) < 50 and match_count > 0:
                    length_factor = 1.1
                
                score = pattern_coverage * position_factor * length_factor * weight
                intent_scores[intent] = score
        
        # Normalize scores and create categories
        if intent_scores:
            max_score = max(intent_scores.values())
            
            for intent, score in intent_scores.items():
                # Calculate confidence (normalized score with minimum threshold)
                normalized_score = score / max_score
                confidence = min(max(0.6, 0.5 + (normalized_score * 0.4)), 0.95)
                
                categories.append({
                    "category_id": f"{conversation['id']}_intent_{intent}",
                    "category_type": "intent",
                    "category_value": intent,
                    "confidence_score": confidence,
                    "created_at": datetime.now().isoformat(),
                    "created_by": "system"
                })
        
        return categories
    
    def _analyze_sentiment(
        self,
        content: str,
        conversation: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Analyze sentiment in content.
        
        Args:
            content: Text content to analyze
            conversation: Conversation data
            
        Returns:
            List of sentiment category objects
        """
        content_lower = content.lower()
        
        # Define sentiment lexicons with weights
        sentiment_lexicon = {
            "positive": {
                "strong": [
                    "excellent", "amazing", "awesome", "fantastic", "outstanding",
                    "exceptional", "wonderful", "brilliant", "superb", "perfect",
                    "love", "delighted", "thrilled", "impressed"
                ],
                "moderate": [
                    "good", "great", "happy", "pleased", "satisfied", "like",
                    "helpful", "appreciate", "grateful", "nice", "enjoy",
                    "thank you", "thanks", "positive", "well done", "effective"
                ],
                "weak": [
                    "ok", "okay", "fine", "alright", "decent", "acceptable",
                    "adequate", "satisfactory", "sufficient", "reasonable"
                ]
            },
            "negative": {
                "strong": [
                    "terrible", "awful", "horrible", "dreadful", "abysmal",
                    "hate", "furious", "outraged", "disgusted", "appalled",
                    "unacceptable", "useless", "pathetic", "disaster"
                ],
                "moderate": [
                    "bad", "poor", "disappointing", "frustrated", "angry",
                    "unhappy", "dissatisfied", "dislike", "annoyed", "irritated",
                    "problem", "issue", "error", "bug", "crash", "not working", "broken"
                ],
                "weak": [
                    "not great", "could be better", "mediocre", "subpar",
                    "underwhelming", "lacking", "insufficient", "not ideal"
                ]
            }
        }
        
        # Negation words that can flip sentiment
        negation_words = [
            "not", "no", "never", "neither", "nor", "none", "nothing",
            "nowhere", "hardly", "barely", "scarcely", "doesn't", "don't",
            "didn't", "isn't", "aren't", "wasn't", "weren't", "hasn't",
            "haven't", "hadn't", "won't", "wouldn't", "can't", "cannot",
            "couldn't", "shouldn't"
        ]
        
        # Intensifiers that strengthen sentiment
        intensifiers = [
            "very", "really", "extremely", "incredibly", "absolutely",
            "completely", "totally", "utterly", "highly", "especially",
            "particularly", "exceptionally", "remarkably", "decidedly",
            "exceedingly", "immensely", "thoroughly", "entirely", "fully"
        ]
        
        # Calculate sentiment scores with context awareness
        positive_score = 0
        negative_score = 0
        
        # Split content into sentences for more accurate negation handling
        sentences = re.split(r'[.!?]+', content_lower)
        
        for sentence in sentences:
            words = sentence.split()
            
            # Check for negation in this sentence
            has_negation = any(neg in words for neg in negation_words)
            
            # Check for intensifiers
            intensifier_count = sum(1 for word in words if word in intensifiers)
            intensifier_multiplier = 1.0 + (0.2 * intensifier_count)
            
            # Process positive sentiment
            for strength, terms in sentiment_lexicon["positive"].items():
                # Assign weight based on strength
                if strength == "strong":
                    weight = 3.0
                elif strength == "moderate":
                    weight = 2.0
                else:  # weak
                    weight = 1.0
                
                # Count occurrences
                for term in terms:
                    if term in sentence:
                        count = sentence.count(term)
                        
                        # Apply negation (flips positive to negative)
                        if has_negation:
                            negative_score += count * weight * intensifier_multiplier
                        else:
                            positive_score += count * weight * intensifier_multiplier
            
            # Process negative sentiment
            for strength, terms in sentiment_lexicon["negative"].items():
                # Assign weight based on strength
                if strength == "strong":
                    weight = 3.0
                elif strength == "moderate":
                    weight = 2.0
                else:  # weak
                    weight = 1.0
                
                # Count occurrences
                for term in terms:
                    if term in sentence:
                        count = sentence.count(term)
                        
                        # Apply negation (flips negative to positive)
                        if has_negation:
                            positive_score += count * weight * intensifier_multiplier
                        else:
                            negative_score += count * weight * intensifier_multiplier
        
        # Consider message length for normalization
        content_length = len(content_lower)
        length_factor = min(1.0, max(0.5, content_length / 500))
        
        # Normalize scores
        positive_score *= length_factor
        negative_score *= length_factor
        
        # Determine sentiment
        if positive_score > negative_score * 1.2:  # Positive needs to be clearly stronger
            sentiment = "positive"
            difference = positive_score - negative_score
            confidence = min(0.6 + (difference * 0.05), 0.95)
        elif negative_score > positive_score * 1.1:  # Negative is stronger
            sentiment = "negative"
            difference = negative_score - positive_score
            confidence = min(0.6 + (difference * 0.05), 0.95)
        else:
            # Mixed or neutral sentiment
            if positive_score > 0 and negative_score > 0:
                sentiment = "mixed"
                # Lower confidence for mixed sentiment
                total = positive_score + negative_score
                balance = 1.0 - (abs(positive_score - negative_score) / total)
                confidence = 0.5 + (balance * 0.3)
            else:
                sentiment = "neutral"
                confidence = 0.7
        
        return [{
            "category_id": f"{conversation['id']}_sentiment_{sentiment}",
            "category_type": "sentiment",
            "category_value": sentiment,
            "confidence_score": confidence,
            "created_at": datetime.now().isoformat(),
            "created_by": "system"
        }]
    
    def extract_categories_batch(self, conversations: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract categories for a batch of conversations.
        
        Args:
            conversations: List of conversation documents
            
        Returns:
            Dictionary mapping conversation IDs to lists of category objects
        """
        # Extract user messages for analysis
        conversation_texts = []
        conversation_ids = []
        
        for conversation in conversations:
            user_messages = [msg for msg in conversation.get('messages', []) if msg.get('role') == 'user']
            user_content = " ".join([msg.get('content', '') for msg in user_messages])
            
            conversation_texts.append(user_content)
            conversation_ids.append(conversation.get('_id'))
        
        # Process in parallel
        categories_by_conversation = {}
        
        def process_conversation(idx):
            conversation_id = conversation_ids[idx]
            text = conversation_texts[idx]
            conversation = next((c for c in conversations if c.get('_id') == conversation_id), {})
            
            # Extract categories
            topic_categories = self._extract_topic_categories(text, conversation)
            intent_categories = self._extract_intent_categories(text, conversation)
            sentiment_categories = self._analyze_sentiment(text, conversation)
            
            # Combine categories
            all_categories = topic_categories + intent_categories + sentiment_categories
            
            return conversation_id, all_categories
        
        # Process in parallel using thread pool
        results = thread_pool_manager.map_processing(
            process_conversation,
            list(range(len(conversation_ids)))
        )
        
        # Convert results to dictionary
        for conversation_id, categories in results:
            if conversation_id:
                categories_by_conversation[conversation_id] = categories
        
        return categories_by_conversation
    
    def update_user_analytics_batch(
        self,
        conversations: List[Dict[str, Any]],
        existing_user_analytics: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Update user analytics for a batch of conversations.
        
        Args:
            conversations: List of processed conversation documents
            existing_user_analytics: Dictionary mapping user IDs to existing user analytics
            
        Returns:
            List of updated user analytics documents
        """
        # Group conversations by user
        conversations_by_user = {}
        
        for conversation in conversations:
            user_id = conversation.get('from_end_user_id')
            if not user_id:
                continue
                
            if user_id not in conversations_by_user:
                conversations_by_user[user_id] = []
                
            conversations_by_user[user_id].append(conversation)
        
        # Process each user's conversations
        updated_user_analytics = []
        
        for user_id, user_conversations in conversations_by_user.items():
            # Get existing user analytics
            user_analytics = existing_user_analytics.get(user_id, {
                "_id": user_id,
                "updated_at": datetime.now().isoformat(),
                "total_conversations": 0,
                "total_messages": 0,
                "total_tokens": 0,
                "total_price": 0,
                "first_conversation_at": None,
                "last_conversation_at": None,
                "daily_metrics": {},
                "category_distribution": {},
                "model_usage": {}
            })
            
            # Update user analytics with each conversation
            for conversation in user_conversations:
                # Update first/last conversation timestamps
                conv_created_at = conversation.get('created_at')
                
                if not user_analytics.get('first_conversation_at') or conv_created_at < user_analytics['first_conversation_at']:
                    user_analytics['first_conversation_at'] = conv_created_at
                    
                if not user_analytics.get('last_conversation_at') or conv_created_at > user_analytics['last_conversation_at']:
                    user_analytics['last_conversation_at'] = conv_created_at
                
                # Update total metrics
                user_analytics['total_conversations'] += 1
                user_analytics['total_messages'] += conversation.get('message_count', 0)
                user_analytics['total_tokens'] += conversation.get('total_tokens', 0)
                user_analytics['total_price'] += conversation.get('total_price', 0)
                
                # Update daily metrics
                conv_date = conv_created_at.split('T')[0] if isinstance(conv_created_at, str) else None
                if conv_date:
                    if conv_date not in user_analytics['daily_metrics']:
                        user_analytics['daily_metrics'][conv_date] = {
                            "conversation_count": 0,
                            "message_count": 0,
                            "total_tokens": 0,
                            "total_price": 0
                        }
                        
                    user_analytics['daily_metrics'][conv_date]['conversation_count'] += 1
                    user_analytics['daily_metrics'][conv_date]['message_count'] += conversation.get('message_count', 0)
                    user_analytics['daily_metrics'][conv_date]['total_tokens'] += conversation.get('total_tokens', 0)
                    user_analytics['daily_metrics'][conv_date]['total_price'] += conversation.get('total_price', 0)
                
                # Update category distribution
                for category in conversation.get('categories', []):
                    category_key = f"{category.get('category_type')}:{category.get('category_value')}"
                    user_analytics['category_distribution'][category_key] = user_analytics['category_distribution'].get(category_key, 0) + 1
                
                # Update model usage
                model_id = conversation.get('model_id', 'unknown')
                if model_id not in user_analytics['model_usage']:
                    user_analytics['model_usage'][model_id] = {
                        "conversation_count": 0,
                        "message_count": 0,
                        "total_tokens": 0,
                        "total_price": 0
                    }
                    
                user_analytics['model_usage'][model_id]['conversation_count'] += 1
                user_analytics['model_usage'][model_id]['message_count'] += conversation.get('message_count', 0)
                user_analytics['model_usage'][model_id]['total_tokens'] += conversation.get('total_tokens', 0)
                user_analytics['model_usage'][model_id]['total_price'] += conversation.get('total_price', 0)
            
            # Update timestamp
            user_analytics['updated_at'] = datetime.now().isoformat()
            
            updated_user_analytics.append(user_analytics)
        
        return updated_user_analytics
    
    def generate_analytics_reports(
        self,
        conversations: List[Dict[str, Any]],
        report_type: str = "daily"
    ) -> List[Dict[str, Any]]:
        """
        Generate analytics reports for a batch of conversations.
        
        Args:
            conversations: List of processed conversation documents
            report_type: Type of report ("daily", "weekly", "monthly")
            
        Returns:
            List of analytics report documents
        """
        if not conversations:
            return []
        
        # Group conversations by date
        conversations_by_date = {}
        
        for conversation in conversations:
            conv_created_at = conversation.get('created_at')
            if not conv_created_at:
                continue
                
            # Extract date components
            if isinstance(conv_created_at, str):
                date_parts = conv_created_at.split('T')[0].split('-')
                if len(date_parts) != 3:
                    continue
                    
                year, month, day = date_parts
                
                if report_type == "daily":
                    period_key = f"{year}-{month}-{day}"
                    period_start = f"{year}-{month}-{day}T00:00:00Z"
                    period_end = f"{year}-{month}-{day}T23:59:59Z"
                elif report_type == "weekly":
                    # This is a simplified weekly grouping by the day of the week
                    # In a real implementation, you would use proper week calculations
                    import datetime as dt
                    date_obj = dt.date(int(year), int(month), int(day))
                    week_start = date_obj - dt.timedelta(days=date_obj.weekday())
                    week_end = week_start + dt.timedelta(days=6)
                    period_key = f"{week_start.year}-W{week_start.isocalendar()[1]}"
                    period_start = f"{week_start.isoformat()}T00:00:00Z"
                    period_end = f"{week_end.isoformat()}T23:59:59Z"
                elif report_type == "monthly":
                    period_key = f"{year}-{month}"
                    period_start = f"{year}-{month}-01T00:00:00Z"
                    # Simplified month end calculation
                    if month == "12":
                        period_end = f"{int(year)+1}-01-01T00:00:00Z"
                    else:
                        next_month = int(month) + 1
                        period_end = f"{year}-{next_month:02d}-01T00:00:00Z"
                else:
                    # Default to daily
                    period_key = f"{year}-{month}-{day}"
                    period_start = f"{year}-{month}-{day}T00:00:00Z"
                    period_end = f"{year}-{month}-{day}T23:59:59Z"
                
                if period_key not in conversations_by_date:
                    conversations_by_date[period_key] = {
                        "conversations": [],
                        "period_start": period_start,
                        "period_end": period_end
                    }
                
                conversations_by_date[period_key]["conversations"].append(conversation)
        
        # Generate reports for each period
        reports = []
        
        for period_key, period_data in conversations_by_date.items():
            period_conversations = period_data["conversations"]
            
            # Skip periods with no conversations
            if not period_conversations:
                continue
            
            # Calculate metrics
            total_conversations = len(period_conversations)
            total_messages = sum(conv.get('message_count', 0) for conv in period_conversations)
            total_tokens = sum(conv.get('total_tokens', 0) for conv in period_conversations)
            total_price = sum(conv.get('total_price', 0) for conv in period_conversations)
            
            # Count unique users
            unique_users = set(conv.get('from_end_user_id') for conv in period_conversations if conv.get('from_end_user_id'))
            unique_user_count = len(unique_users)
            
            # Count by model
            model_counts = {}
            for conv in period_conversations:
                model_id = conv.get('model_id', 'unknown')
                if model_id not in model_counts:
                    model_counts[model_id] = {
                        "conversation_count": 0,
                        "message_count": 0,
                        "total_tokens": 0,
                        "total_price": 0
                    }
                
                model_counts[model_id]["conversation_count"] += 1
                model_counts[model_id]["message_count"] += conv.get('message_count', 0)
                model_counts[model_id]["total_tokens"] += conv.get('total_tokens', 0)
                model_counts[model_id]["total_price"] += conv.get('total_price', 0)
            
            # Count by category
            category_counts = {}
            for conv in period_conversations:
                for category in conv.get('categories', []):
                    category_key = f"{category.get('category_type')}:{category.get('category_value')}"
                    category_counts[category_key] = category_counts.get(category_key, 0) + 1
            
            # Create report document
            report = {
                "_id": f"{report_type}_{period_key}",
                "report_type": report_type,
                "period_key": period_key,
                "period_start": period_data["period_start"],
                "period_end": period_data["period_end"],
                "created_at": datetime.now().isoformat(),
                "report_data": {
                    "total_conversations": total_conversations,
                    "total_messages": total_messages,
                    "total_tokens": total_tokens,
                    "total_price": total_price,
                    "unique_user_count": unique_user_count,
                    "average_messages_per_conversation": total_messages / total_conversations if total_conversations > 0 else 0,
                    "average_tokens_per_conversation": total_tokens / total_conversations if total_conversations > 0 else 0,
                    "average_price_per_conversation": total_price / total_conversations if total_conversations > 0 else 0,
                    "model_counts": model_counts,
                    "category_counts": category_counts
                }
            }
            
            reports.append(report)
        
        return reports
