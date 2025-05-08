# categorize.py
import openai

CATEGORIES = [
    "Politics & Government",
    "Business & Finance",
    "Technology",
    "Health",
    "Science & Environment",
    "Sports",
    "Entertainment",
    "Crime & Law",
    "Lifestyle",
    "World News",
    "Education",
    "Opinion & Editorials"
]

client = None

def set_openai_api_key(api_key):
    global client
    client = openai.OpenAI(api_key=api_key)

def categorize_text(text):
    if client is None:
        raise Exception("OpenAI client not initialized. Call set_openai_api_key() first.")

    try:
        prompt = f"""
Given the following news post, do the following:

1. Categorize it into one of these categories: {', '.join(CATEGORIES)}.
2. Rate how controversial it is on a scale from 1 to 10 (1 = not controversial, 10 = extremely controversial).

Respond in this JSON format:
{{"category": "your-category", "controversy": number}}

Post:
\"{text}\"
"""
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=100
        )

        content = response.choices[0].message.content.strip()

        # Parse the JSON-like response safely
        import json
        result = json.loads(content)
        category = result.get("category", "Uncategorized")
        controversy = int(result.get("controversy", 1))
        return category, controversy

    except Exception as e:
        print(f"❌ Categorization failed: {e}")
        return "Uncategorized", 1
