# Fetch Conversation Messages by Date

This script fetches all conversations created on a specific day from MongoDB and extracts only the messages from those conversations. By default, it fetches yesterday's conversations, but you can specify a different number of days ago. It's useful for data analysis, reporting, and archiving purposes.

## Features

- Retrieves all conversations created during a specific day (00:00:00 to 23:59:59)
- Allows specifying how many days ago to fetch (default: 1 for yesterday)
- Groups messages by conversation for easier analysis
- Outputs conversations with their messages to console or saves them to a JSON file
- Supports custom MongoDB connection parameters
- Automatically creates output directories if they don't exist

## Prerequisites

- Python 3.8+
- Access to a MongoDB instance containing conversation data
- The analytics_framework package (included in this repository)

## Configuration

The script uses the following configuration from the analytics_framework:

- `MONGODB_URI`: MongoDB connection URI
- `MONGODB_DATABASE`: MongoDB database name

These can be overridden using command-line arguments.

## Usage

### Basic Usage

```bash
# On Windows
python scripts/fetch_yesterday_messages.py

# On Unix-like systems (if script has execute permissions)
./scripts/fetch_yesterday_messages.py
```

This will fetch yesterday's conversations, extract the messages, and print them to the console in JSON format.

### Fetch Conversations from a Specific Day

```bash
# Fetch conversations from 3 days ago
python scripts/fetch_yesterday_messages.py --days-ago 3

# Short form
python scripts/fetch_yesterday_messages.py -d 3
```

This will fetch conversations from 3 days ago instead of yesterday.

### Save Output to a File

```bash
python scripts/fetch_yesterday_messages.py --output yesterday_messages.json

# Combine with days-ago
python scripts/fetch_yesterday_messages.py --days-ago 5 --output five_days_ago_messages.json
```

This will save the messages to a JSON file instead of printing them to the console.

### Custom MongoDB Connection

```bash
python scripts/fetch_yesterday_messages.py --mongodb-uri "mongodb://user:pass@host:port" --mongodb-database "my_db"
```

This allows you to specify a custom MongoDB connection URI and database name.

### All Options

```bash
python scripts/fetch_yesterday_messages.py --help
```

This will display all available command-line options.

## Output Format

The script outputs a list of conversation objects, each containing:

- Conversation metadata (ID, name, app ID, model ID, user ID, creation time)
- The conversation's inputs object
- A list of all messages in that conversation with comprehensive message data
- A formatted HTML representation of the conversation for easy display

Example output:

```json
[
  {
    "conversation_id": "12345",
    "conversation_name": "Support Chat",
    "app_id": "app_123",
    "model_id": "gpt-4",
    "from_end_user_id": "user_456",
    "created_at": "2025-03-16T10:30:00",
    "inputs": {
      "question": "I need help with my account",
      "context": "Premium customer since 2023"
    },
    "message_count": 2,
    "messages": [
      {
        "message_id": "msg_789",
        "sequence_number": 1,
        "role": "user",
        "content": "Hello, I need help with my account",
        "query": "Hello, I need help with my account",
        "message": "Hello, I need help with my account",
        "answer": null,
        "tokens": 10,
        "message_tokens": 10,
        "answer_tokens": null,
        "price": 0.0002,
        "total_price": 0.0002,
        "created_at": "2025-03-16T10:30:05",
        "parent_message_id": null,
        "message_metadata": {
          "source": "web",
          "browser": "Chrome"
        }
      },
      {
        "message_id": "msg_790",
        "sequence_number": 2,
        "role": "assistant",
        "content": "Hi there! I'd be happy to help with your account. What specific issue are you experiencing?",
        "query": null,
        "message": null,
        "answer": "Hi there! I'd be happy to help with your account. What specific issue are you experiencing?",
        "tokens": 20,
        "message_tokens": null,
        "answer_tokens": 20,
        "price": 0.0004,
        "total_price": 0.0004,
        "created_at": "2025-03-16T10:30:10",
        "parent_message_id": "msg_789",
        "message_metadata": null
      }
    ],
    "formatted_conversation": "Time: 2025-03-16T10:30:05 <b>User: User</b> Message: Hello, I need help with my account <br>Time: 2025-03-16T10:30:10 <b>User: AI</b> Message: Hi there! I'd be happy to help with your account. What specific issue are you experiencing? <br>"
  }
]
```

### Formatted Conversation

The `formatted_conversation` field provides a human-readable HTML representation of the conversation, which can be directly displayed in web applications or reports. The format follows this exact pattern:

```
Time: [timestamp] <b>User: [username]</b> Message: [content] <br>
```

Where:

- `[timestamp]` is the message creation time
- `[username]` is the actual user ID from the conversation for user messages, or "AI" for assistant messages
- `[content]` is the message content, preserved exactly as is (including newlines and markdown formatting)
- Each message is separated by a line break (`<br>`)

Example:

```
Time: 2024-09-12 23:38:28+00:00 <b>User: Yonesp</b> Message: Saya sudah deposit tapi belum masuk <br>Time: 2024-09-12 23:38:28+00:00 <b>User: AI</b> Message: Halo Yonesp, terima kasih telah menghubungi kami.\n\nBerdasarkan riwayat deposit terbaru Anda, berikut adalah statusnya:\n\n- **500.00 IDR** - **Gagal**\n- **2,773.12 IDR** - **Sukses**\n- **783.12 IDR** - **Sukses**\n- **1,000.12 IDR** - **Sukses**\n- **1,000.12 IDR** - **Sukses**\n\nUntuk deposit sebesar **500.00 IDR**, statusnya adalah **Gagal**. Anda dapat mencoba kembali atau memilih metode pembayaran lain. <br>
```

System messages (messages with role="system") are excluded from the formatted conversation as they typically contain instructions or context for the AI rather than being part of the user-visible conversation.

### Content Field Handling

The script intelligently handles different field names for message content:

1. It first checks for a `content` field in the message
2. If not found, it looks for `message`, `answer`, or `query` fields
3. All original fields are preserved in the output for reference
4. If no content is found in any field, it defaults to an empty string

This ensures that message content is always available in the `content` field, regardless of how it was stored in the original data.

This format groups all messages by their conversation, making it easier to analyze complete conversation flows.

## Integration with Other Tools

This script can be integrated with other tools and workflows:

- Schedule it to run daily using cron or Windows Task Scheduler
- Pipe the output to other data processing tools
- Use it as part of a data pipeline for analytics or reporting
- Combine with other scripts to generate daily reports

## Troubleshooting

If you encounter issues:

1. Ensure MongoDB is running and accessible
2. Check that the MongoDB URI and database name are correct
3. Verify that you have the necessary permissions to access the conversations collection
4. Check the logs for detailed error messages

## License

This script is part of the Conversation Analytics Framework and is subject to the same license as the main project.
